/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.formats.lance;

import com.qcloud.cos.auth.InstanceProfileCredentials;
import com.tencent.cloud.tclake.TCLakeCOSCredentialsProvider;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provider of temporary Tencent Cloud COS credentials for Lance.
 *
 * <p>Under the hood it requests temporary COS credentials from TCLake via {@link
 * TCLakeCOSCredentialsProvider}. Each Lance table location maps to a different bucket (e.g. {@code
 * cosn://bucket-a/...} and {@code cosn://bucket-b/...}), and TCLake uses {@code URI.getHost()} as
 * the {@code X-Bucket} header when issuing credentials. Therefore tables under the same catalog
 * must build the delegate with their own bucket to request credentials; otherwise a bucket mismatch
 * leads to a 403 PermissionDenied.
 *
 * <p>Caching strategy:
 *
 * <ul>
 *   <li>a <b>process-wide global cache</b> keyed by bucket (lowercased), so credentials are not
 *       lost when a catalog is re-initialized;
 *   <li>each bucket's credentials are refreshed early at <b>50% of their actual lifetime</b> (i.e.
 *       once half of the window has elapsed); when the expiration time is unavailable it falls back
 *       to a fixed TTL (default 5 minutes);
 *   <li>a {@link ConcurrentHashMap} plus one {@link ReentrantLock} per bucket guarantees that a
 *       given bucket is refreshed only once under concurrency;
 *   <li>refreshes of different buckets do not block each other.
 * </ul>
 */
public class LanceCOSCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(LanceCOSCredentialsProvider.class);

  /**
   * Fallback cache TTL: used only when the credential expiration time is unavailable from TCLake;
   * defaults to 5 minutes.
   */
  private static final Duration DEFAULT_CACHE_TTL = Duration.ofMinutes(5);

  /** Process-wide global cache: bucket(lowercase) -> cache entry. */
  private static final ConcurrentMap<String, CacheEntry> GLOBAL_CACHE = new ConcurrentHashMap<>();

  private final Configuration hadoopConf;
  private final String cosRegion;
  private final Duration cacheTtl;

  /**
   * Construct the provider using the default 5-minute TTL.
   *
   * @param hadoopConf the catalog-level Hadoop Configuration, passed through as-is to the
   *     underlying TCLake provider (containing tclake.cos.credentials.server.url /
   *     tclake.cos.credentials.user.appid, etc.).
   * @param cosRegion the COS region; an empty string is allowed.
   */
  public LanceCOSCredentialsProvider(Configuration hadoopConf, String cosRegion) {
    this(hadoopConf, cosRegion, DEFAULT_CACHE_TTL);
  }

  /**
   * Construct the provider with a specified cache TTL.
   *
   * @param hadoopConf the catalog-level Hadoop Configuration; must not be null.
   * @param cosRegion the COS region; an empty string is allowed.
   * @param cacheTtl the cache lifetime; must be positive.
   */
  public LanceCOSCredentialsProvider(
      Configuration hadoopConf, String cosRegion, Duration cacheTtl) {
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf must not be null");
    this.cosRegion = cosRegion == null ? "" : cosRegion;
    Objects.requireNonNull(cacheTtl, "cacheTtl must not be null");
    if (cacheTtl.isZero() || cacheTtl.isNegative()) {
      throw new IllegalArgumentException("cacheTtl must be positive, got " + cacheTtl);
    }
    this.cacheTtl = cacheTtl;
  }

  /**
   * Get the COS storage options for the bucket of the given table location; the returned value can
   * be used directly as Lance storage options.
   *
   * <p>Internally a global cache is kept per bucket. On a fresh (non-expired) cache hit it is
   * returned directly; otherwise a {@link TCLakeCOSCredentialsProvider} is built for that bucket to
   * request a new temporary credential from TCLake.
   *
   * @param tableLocation the full table location; must be a URI with a scheme, e.g. {@code
   *     cosn://{bucket}/path/to/table} or {@code cos://{bucket}/path/to/table}.
   * @return Lance storage options (region/secretId/secretKey/sessionToken[/expires]).
   * @throws IllegalArgumentException if the location is blank or the bucket cannot be parsed.
   */
  public Map<String, String> getStorageOptions(String tableLocation) {
    String bucket = extractBucket(tableLocation);
    CachedCredentials snapshot = getOrRefresh(bucket, tableLocation);
    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Return Lance COS storage options: bucket={}, region={}, secretIdPrefix={},"
              + " secretIdLen={}, secretKeyLen={}, sessionTokenPrefix={}, sessionTokenLen={},"
              + " issuedAt={}, ageSeconds={}, expireAtMillis={}",
          bucket,
          cosRegion,
          maskPrefix(snapshot.secretId),
          snapshot.secretId.length(),
          snapshot.secretKey.length(),
          maskPrefix(snapshot.securityToken),
          snapshot.securityToken.length(),
          snapshot.issuedAt,
          Duration.between(snapshot.issuedAt, Instant.now()).getSeconds(),
          snapshot.expireAtMillis);
    }
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(LanceProperties.COS_REGION, cosRegion);
    builder.put(LanceProperties.COS_SECRET_ID, snapshot.secretId);
    builder.put(LanceProperties.COS_SECRET_KEY, snapshot.secretKey);
    builder.put(LanceProperties.COS_SECURITY_TOKEN, snapshot.securityToken);
    // The temporary credential expiration time (epoch millis), exposed to the Lance storage option.
    if (snapshot.expireAtMillis > 0) {
      builder.put(LanceProperties.EXPIRES_AT_MILLIS, String.valueOf(snapshot.expireAtMillis));
    }
    return builder.build();
  }

  /** Parse the bucket (i.e. the URI host) from a table location. */
  private static String extractBucket(String tableLocation) {
    if (StringUtils.isBlank(tableLocation)) {
      throw new IllegalArgumentException("tableLocation must not be blank");
    }
    try {
      URI uri = new URI(tableLocation);
      String host = uri.getHost();
      if (StringUtils.isBlank(host)) {
        throw new IllegalArgumentException(
            "Cannot extract bucket from tableLocation: " + tableLocation);
      }
      return host.toLowerCase(Locale.ROOT);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid tableLocation, cannot extract bucket: " + tableLocation, e);
    }
  }

  /** Get the cached snapshot for a bucket, triggering a refresh when necessary. */
  private CachedCredentials getOrRefresh(String bucket, String tableLocation) {
    CacheEntry entry = GLOBAL_CACHE.computeIfAbsent(bucket, k -> new CacheEntry());

    CachedCredentials snapshot = entry.credentials;
    if (snapshot != null && !isExpired(snapshot)) {
      LOG.debug(
          "Lance COS credentials cache hit: bucket={}, issuedAt={}, ageSeconds={}",
          bucket,
          snapshot.issuedAt,
          Duration.between(snapshot.issuedAt, Instant.now()).getSeconds());
      return snapshot;
    }

    entry.lock.lock();
    try {
      // double-check: re-validate after entering the critical section to avoid duplicate refreshes
      snapshot = entry.credentials;
      if (snapshot == null || isExpired(snapshot)) {
        if (snapshot == null) {
          LOG.info(
              "Lance COS credentials not initialized for bucket={}, loading for the first time"
                  + " from tableLocation={}",
              bucket,
              tableLocation);
        } else {
          LOG.info(
              "Lance COS credentials expired for bucket={} (issuedAt={}, ttl={}), refreshing"
                  + " from tableLocation={}",
              bucket,
              snapshot.issuedAt,
              cacheTtl,
              tableLocation);
        }
        snapshot = loadFromTCLake(bucket, tableLocation);
        entry.credentials = snapshot;
      } else {
        LOG.debug(
            "Lance COS credentials cache hit after double-check: bucket={}, issuedAt={}",
            bucket,
            snapshot.issuedAt);
      }
      return snapshot;
    } finally {
      entry.lock.unlock();
    }
  }

  /**
   * Decide whether the cached snapshot needs to be refreshed.
   *
   * <p>Expiration policy: a temporary credential is refreshed early at <b>50% of its actual
   * lifetime</b> — i.e. once more than half of the window from issuance to the TCLake-returned
   * expiration time has elapsed, it is considered expired. This leaves enough buffer for subsequent
   * COS requests and avoids using a credential that is about to expire.
   *
   * <p>When a valid expiration time is unavailable ({@code expireAtMillis <= 0} or not later than
   * the issuance time), it falls back to the fixed {@link #cacheTtl}.
   */
  private boolean isExpired(CachedCredentials snapshot) {
    long nowMillis = System.currentTimeMillis();
    long issuedMillis = snapshot.issuedAt.toEpochMilli();
    if (snapshot.expireAtMillis > issuedMillis) {
      long halfLifeMillis = issuedMillis + (snapshot.expireAtMillis - issuedMillis) / 2;
      return nowMillis >= halfLifeMillis;
    }
    // Fall back to the fixed TTL when no valid expiration time is available.
    return Duration.between(snapshot.issuedAt, Instant.now()).compareTo(cacheTtl) >= 0;
  }

  /**
   * Build the underlying {@link TCLakeCOSCredentialsProvider} from the table location (the URI host
   * is the bucket) and request a new temporary credential from TCLake.
   *
   * <p>Note: a new TCLakeCOSCredentialsProvider instance is created on every refresh. The instance
   * is very lightweight, and this is required to ensure the {@code X-Bucket} header in the TCLake
   * request points to the correct target bucket.
   */
  private CachedCredentials loadFromTCLake(String bucket, String tableLocation) {
    URI uri;
    try {
      uri = new URI(tableLocation);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid tableLocation for TCLakeCOSCredentialsProvider: " + tableLocation, e);
    }
    LOG.info(
        "Requesting new Lance COS credentials from TCLake: bucket={}, uri={}, region={}",
        bucket,
        uri,
        cosRegion);
    long startNanos = System.nanoTime();
    TCLakeCOSCredentialsProvider delegate;
    try {
      delegate = new TCLakeCOSCredentialsProvider(uri, hadoopConf);
    } catch (Exception e) {
      LOG.error(
          "Failed to construct TCLakeCOSCredentialsProvider: bucket={}, uri={}, errorType={},"
              + " message={}",
          bucket,
          uri,
          e.getClass().getName(),
          e.getMessage(),
          e);
      throw new RuntimeException(
          "Failed to construct TCLakeCOSCredentialsProvider for bucket " + bucket, e);
    }

    InstanceCredentialsSnapshot credentials;
    try {
      InstanceProfileCredentials rawCredentials =
          (InstanceProfileCredentials) delegate.getCredentials();
      String accessKeyId = rawCredentials.getCOSAccessKeyId();
      String rawSecretKey = rawCredentials.getCOSSecretKey();
      String sessionToken = rawCredentials.getSessionToken();
      long expireTimeInMS = rawCredentials.getExpiredTime() * 1000;
      credentials =
          new InstanceCredentialsSnapshot(accessKeyId, rawSecretKey, sessionToken, expireTimeInMS);
    } catch (RuntimeException e) {
      LOG.error(
          "Failed to fetch Lance COS credentials from TCLake: bucket={}, uri={}, region={},"
              + " elapsedMs={}, errorType={}, message={}",
          bucket,
          uri,
          cosRegion,
          Duration.ofNanos(System.nanoTime() - startNanos).toMillis(),
          e.getClass().getName(),
          e.getMessage(),
          e);
      throw e;
    }
    long elapsedMs = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();

    String secretId = credentials.secretId;
    String secretKey = credentials.secretKey;
    String securityToken = credentials.securityToken;

    Instant now = Instant.now();
    CachedCredentials snapshot =
        new CachedCredentials(secretId, secretKey, securityToken, now, credentials.expireAtMillis);

    LOG.info(
        "Refreshed Lance COS credentials: bucket={}, issuedAt={}, expireAtMillis={},"
            + " cacheTtlSeconds={}, elapsedMs={},"
            + " region={}, secretIdPrefix={}, secretIdLen={}, secretIdBlank={}, secretKeyLen={},"
            + " secretKeyBlank={}, sessionTokenPrefix={}, sessionTokenLen={}, sessionTokenBlank={}",
        bucket,
        snapshot.issuedAt,
        snapshot.expireAtMillis,
        cacheTtl.getSeconds(),
        elapsedMs,
        cosRegion,
        maskPrefix(snapshot.secretId),
        snapshot.secretId.length(),
        snapshot.secretId.isEmpty(),
        snapshot.secretKey.length(),
        snapshot.secretKey.isEmpty(),
        maskPrefix(snapshot.securityToken),
        snapshot.securityToken.length(),
        snapshot.securityToken.isEmpty());

    if (snapshot.secretId.isEmpty()
        || snapshot.secretKey.isEmpty()
        || snapshot.securityToken.isEmpty()) {
      LOG.warn(
          "Lance COS credentials returned from TCLake contain blank field(s); subsequent COS"
              + " operations will likely fail with 403. bucket={}, region={}, secretIdBlank={},"
              + " secretKeyBlank={}, sessionTokenBlank={}",
          bucket,
          cosRegion,
          snapshot.secretId.isEmpty(),
          snapshot.secretKey.isEmpty(),
          snapshot.securityToken.isEmpty());
    }
    return snapshot;
  }

  /**
   * Mask a sensitive string: keep only the first 4 chars followed by ***; return "<empty>" for
   * blank.
   */
  private static String maskPrefix(String value) {
    if (value == null || value.isEmpty()) {
      return "<empty>";
    }
    if (value.length() <= 4) {
      return "***";
    }
    return value.substring(0, 4) + "***";
  }

  /** One cache entry per bucket, holding a credential snapshot and a lock used for refresh. */
  private static final class CacheEntry {
    private final ReentrantLock lock = new ReentrantLock();
    private volatile CachedCredentials credentials;
  }

  /**
   * An immutable snapshot extracted from the underlying COSCredentials, so other methods do not
   * depend directly on the concrete qcloud type.
   */
  private static final class InstanceCredentialsSnapshot {
    private final String secretId;
    private final String secretKey;
    private final String securityToken;

    /** Temporary credential expiration time (epoch millis) from TCLake; 0 when unavailable. */
    private final long expireAtMillis;

    InstanceCredentialsSnapshot(
        String secretId, String secretKey, String securityToken, long expireAtMillis) {
      this.secretId = secretId == null ? "" : secretId;
      this.secretKey = secretKey == null ? "" : secretKey;
      this.securityToken = securityToken == null ? "" : securityToken;
      this.expireAtMillis = expireAtMillis;
    }
  }

  /** A cached credential snapshot. */
  private static final class CachedCredentials {
    private final String secretId;
    private final String secretKey;
    private final String securityToken;
    private final Instant issuedAt;

    /**
     * Temporary credential expiration time (epoch millis) from TCLake; 0 when unavailable. Used for
     * the 50%-lifetime refresh decision and exposed to Lance.
     */
    private final long expireAtMillis;

    CachedCredentials(
        String secretId,
        String secretKey,
        String securityToken,
        Instant issuedAt,
        long expireAtMillis) {
      this.secretId = secretId == null ? "" : secretId;
      this.secretKey = secretKey == null ? "" : secretKey;
      this.securityToken = securityToken == null ? "" : securityToken;
      this.issuedAt = issuedAt;
      this.expireAtMillis = expireAtMillis;
    }
  }
}
