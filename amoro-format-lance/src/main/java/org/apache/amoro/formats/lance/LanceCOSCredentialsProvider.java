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
 * Lance COS 临时密钥提供者。
 *
 * <p>底层通过 {@link TCLakeCOSCredentialsProvider} 向 TCLake 申请腾讯云 COS 临时密钥。每张 Lance 表的 location 对应不同的
 * bucket（例如 {@code cosn://bucket-a/...} 与 {@code cosn://bucket-b/...}），而 TCLake 在申请时会使用 {@code
 * URI.getHost()} 作为 {@code X-Bucket} header，因此同一个 catalog 下的多张表必须分别以各自的 bucket 构造 delegate
 * 申请秘钥，否则会因为 bucket 不匹配导致 403 PermissionDenied。
 *
 * <p>缓存策略：
 *
 * <ul>
 *   <li>以 bucket（小写）为 key 做 <b>进程级全局缓存</b>，避免 catalog 重新 initialize 时缓存丢失；
 *   <li>每个 bucket 的秘钥固定缓存 TTL（默认 5 分钟），过期后重新申请；
 *   <li>使用 {@link ConcurrentHashMap} + 每 bucket 一把 {@link ReentrantLock} 保证并发下同一 bucket 仅刷新一次；
 *   <li>不同 bucket 之间刷新互不阻塞。
 * </ul>
 */
public class LanceCOSCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(LanceCOSCredentialsProvider.class);

  /** 默认缓存 TTL：5 分钟。 */
  private static final Duration DEFAULT_CACHE_TTL = Duration.ofMinutes(5);

  /** 进程级全局缓存：bucket(lowercase) -> 缓存条目。 */
  private static final ConcurrentMap<String, CacheEntry> GLOBAL_CACHE = new ConcurrentHashMap<>();

  private final Configuration hadoopConf;
  private final String cosRegion;
  private final Duration cacheTtl;

  /**
   * 构造 provider，使用默认 5 分钟 TTL。
   *
   * @param hadoopConf catalog 级别的 Hadoop Configuration，会原样透传给底层 TCLake provider（其中包含
   *     tclake.cos.credentials.server.url / tclake.cos.credentials.user.appid 等配置）。
   * @param cosRegion COS 区域，允许为空字符串。
   */
  public LanceCOSCredentialsProvider(Configuration hadoopConf, String cosRegion) {
    this(hadoopConf, cosRegion, DEFAULT_CACHE_TTL);
  }

  /**
   * 构造 provider，并指定缓存 TTL。
   *
   * @param hadoopConf catalog 级别的 Hadoop Configuration，不能为 null。
   * @param cosRegion COS 区域，允许为空字符串。
   * @param cacheTtl 缓存有效期，必须为正数。
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
   * 获取指定表 location 对应 bucket 的 COS 存储选项，返回值可直接作为 Lance storage options 使用。
   *
   * <p>内部以 bucket 为维度做全局缓存，若缓存命中且未过期则直接返回；否则以该 bucket 构造一个 {@link TCLakeCOSCredentialsProvider} 向
   * TCLake 申请一份新的临时密钥。
   *
   * @param tableLocation 表的完整 location，必须是带 scheme 的 URI，例如 {@code cosn://{bucket}/path/to/table} 或
   *     {@code cos://{bucket}/path/to/table}。
   * @return Lance storage options（region/secretId/secretKey/sessionToken）。
   * @throws IllegalArgumentException location 为空或无法解析出 bucket 时抛出。
   */
  public Map<String, String> getStorageOptions(String tableLocation) {
    String bucket = extractBucket(tableLocation);
    CachedCredentials snapshot = getOrRefresh(bucket, tableLocation);
    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Return Lance COS storage options: bucket={}, region={}, secretIdPrefix={},"
              + " secretIdLen={}, secretKeyLen={}, sessionTokenPrefix={}, sessionTokenLen={},"
              + " issuedAt={}, ageSeconds={}",
          bucket,
          cosRegion,
          maskPrefix(snapshot.secretId),
          snapshot.secretId.length(),
          snapshot.secretKey.length(),
          maskPrefix(snapshot.securityToken),
          snapshot.securityToken.length(),
          snapshot.issuedAt,
          Duration.between(snapshot.issuedAt, Instant.now()).getSeconds());
    }
    return ImmutableMap.of(
        LanceProperties.COS_REGION, cosRegion,
        LanceProperties.COS_SECRET_ID, snapshot.secretId,
        LanceProperties.COS_SECRET_KEY, snapshot.secretKey,
        LanceProperties.COS_SECURITY_TOKEN, snapshot.securityToken);
  }

  /** 从表 location 中解析 bucket（即 URI 的 host）。 */
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

  /** 按 bucket 维度获取缓存快照，必要时触发刷新。 */
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
      // double-check：进入临界区后再次校验，避免重复刷新
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

  /** 判断缓存是否已超过 TTL。 */
  private boolean isExpired(CachedCredentials snapshot) {
    return Duration.between(snapshot.issuedAt, Instant.now()).compareTo(cacheTtl) >= 0;
  }

  /**
   * 按表 location 构造底层 {@link TCLakeCOSCredentialsProvider}（URI 的 host 即 bucket），向 TCLake 申请一份新的
   * 临时密钥。
   *
   * <p>注意：每次刷新都会新建一个 TCLakeCOSCredentialsProvider 实例。该实例非常轻量，且这是确保 TCLake 请求里 {@code X-Bucket}
   * header 正确指向目标 bucket 的必要手段。
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
      credentials = new InstanceCredentialsSnapshot(accessKeyId, rawSecretKey, sessionToken);
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
    CachedCredentials snapshot = new CachedCredentials(secretId, secretKey, securityToken, now);

    LOG.info(
        "Refreshed Lance COS credentials: bucket={}, issuedAt={}, cacheTtlSeconds={}, elapsedMs={},"
            + " region={}, secretIdPrefix={}, secretIdLen={}, secretIdBlank={}, secretKeyLen={},"
            + " secretKeyBlank={}, sessionTokenPrefix={}, sessionTokenLen={}, sessionTokenBlank={}",
        bucket,
        snapshot.issuedAt,
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

  /** 对敏感字符串做脱敏：只保留前 4 位，后面统一 ***，空串返回 "<empty>"。 */
  private static String maskPrefix(String value) {
    if (value == null || value.isEmpty()) {
      return "<empty>";
    }
    if (value.length() <= 4) {
      return "***";
    }
    return value.substring(0, 4) + "***";
  }

  /** 每个 bucket 对应一条缓存条目，持有一份密钥快照和一把用于刷新的锁。 */
  private static final class CacheEntry {
    private final ReentrantLock lock = new ReentrantLock();
    private volatile CachedCredentials credentials;
  }

  /** 从底层 COSCredentials 中抽取出的不可变快照，避免其他方法直接依赖 qcloud 的具体类型。 */
  private static final class InstanceCredentialsSnapshot {
    private final String secretId;
    private final String secretKey;
    private final String securityToken;

    InstanceCredentialsSnapshot(String secretId, String secretKey, String securityToken) {
      this.secretId = secretId == null ? "" : secretId;
      this.secretKey = secretKey == null ? "" : secretKey;
      this.securityToken = securityToken == null ? "" : securityToken;
    }
  }

  /** 密钥缓存快照。 */
  private static final class CachedCredentials {
    private final String secretId;
    private final String secretKey;
    private final String securityToken;
    private final Instant issuedAt;

    CachedCredentials(String secretId, String secretKey, String securityToken, Instant issuedAt) {
      this.secretId = secretId == null ? "" : secretId;
      this.secretKey = secretKey == null ? "" : secretKey;
      this.securityToken = securityToken == null ? "" : securityToken;
      this.issuedAt = issuedAt;
    }
  }
}
