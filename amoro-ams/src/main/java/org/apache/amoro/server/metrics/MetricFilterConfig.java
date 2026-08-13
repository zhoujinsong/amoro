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

package org.apache.amoro.server.metrics;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.table.TableOptimizingMetrics;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Field-level whitelist for AMS metric registration.
 *
 * <p>The filter gates which {@code MetricDefine} fields of the four metric classes ({@code
 * TableOptimizingMetrics}, {@code TableSummaryMetrics}, {@code TableOrphanFilesCleaningMetrics},
 * {@code OptimizerGroupMetrics}) are actually registered with the {@link MetricRegistry}. The
 * metric instances themselves are still constructed unconditionally — only the registration step is
 * gated. See the design document {@code docs/软件设计/2026-05-29-AMS指标体系精简/} for rationale.
 *
 * <p>Lifecycle: {@link #init(Configurations)} must be called once at AMS bootstrap, before any
 * metric class is instantiated. After that, each metric class reads the singleton via {@link
 * #current()}. {@code AmsServiceMetrics} is intentionally not gated (process-level metrics are
 * cheap and always allowed).
 */
public final class MetricFilterConfig {

  private static final Logger LOG = LoggerFactory.getLogger(MetricFilterConfig.class);

  /** Pre-baked profile selector. */
  public enum Profile {
    /** Registers exactly the KEEP metrics from the design document §3.8. */
    RECOMMENDED,
    /** Registers everything that was registered before this change (60 metrics). */
    LEGACY,
    /** Registers an even tighter subset for very high-cardinality deployments. */
    MINIMAL,
    /** Honours the four {@code *.fields} lists from {@link AmoroManagementConf} verbatim. */
    CUSTOM;

    static Profile parse(String raw) {
      if (raw == null) {
        return LEGACY;
      }
      String normalized = raw.trim().toUpperCase(Locale.ROOT);
      try {
        return Profile.valueOf(normalized);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid metric.profile value: '"
                + raw
                + "'. Valid values are: recommended, legacy, minimal, custom.");
      }
    }
  }

  // -------- Field-name constants exposed to metric classes ---------------------------------
  // The string values are exactly the part of the metric name *after* the per-class prefix.
  // They are the wire format of the four custom-profile config lists.

  // OptimizerGroupMetrics fields (10 total)
  public static final String F_GROUP_PENDING_TASKS = "pending_tasks";
  public static final String F_GROUP_EXECUTING_TASKS = "executing_tasks";
  public static final String F_GROUP_PLANING_TABLES = "planing_tables";
  public static final String F_GROUP_PENDING_TABLES = "pending_tables";
  public static final String F_GROUP_EXECUTING_TABLES = "executing_tables";
  public static final String F_GROUP_IDLE_TABLES = "idle_tables";
  public static final String F_GROUP_COMMITTING_TABLES = "committing_tables";
  public static final String F_GROUP_OPTIMIZER_INSTANCES = "optimizer_instances";
  public static final String F_GROUP_MEMORY_BYTES_ALLOCATED = "memory_bytes_allocated";
  public static final String F_GROUP_THREADS = "threads";

  // TableOptimizingMetrics fields (23 total)
  public static final String F_OPT_STATUS_IDLE_DURATION = "status_idle_duration";
  public static final String F_OPT_STATUS_PENDING_DURATION = "status_pending_duration";
  public static final String F_OPT_STATUS_PLANNING_DURATION = "status_planning_duration";
  public static final String F_OPT_STATUS_EXECUTING_DURATION = "status_executing_duration";
  public static final String F_OPT_STATUS_COMMITTING_DURATION = "status_committing_duration";
  public static final String F_OPT_STATUS_IN_IDLE = "status_in_idle";
  public static final String F_OPT_STATUS_IN_PENDING = "status_in_pending";
  public static final String F_OPT_STATUS_IN_PLANNING = "status_in_planning";
  public static final String F_OPT_STATUS_IN_EXECUTING = "status_in_executing";
  public static final String F_OPT_STATUS_IN_COMMITTING = "status_in_committing";
  public static final String F_OPT_PROCESS_TOTAL_COUNT = "process_total_count";
  public static final String F_OPT_PROCESS_FAILED_COUNT = "process_failed_count";
  public static final String F_OPT_MINOR_TOTAL_COUNT = "minor_total_count";
  public static final String F_OPT_MINOR_FAILED_COUNT = "minor_failed_count";
  public static final String F_OPT_MAJOR_TOTAL_COUNT = "major_total_count";
  public static final String F_OPT_MAJOR_FAILED_COUNT = "major_failed_count";
  public static final String F_OPT_FULL_TOTAL_COUNT = "full_total_count";
  public static final String F_OPT_FULL_FAILED_COUNT = "full_failed_count";
  public static final String F_OPT_PROCESS_CONSECUTIVE_FAILURES = "process_consecutive_failures";
  public static final String F_OPT_SINCE_LAST_MINOR_OPTIMIZATION = "since_last_minor_optimization";
  public static final String F_OPT_SINCE_LAST_MAJOR_OPTIMIZATION = "since_last_major_optimization";
  public static final String F_OPT_SINCE_LAST_FULL_OPTIMIZATION = "since_last_full_optimization";
  public static final String F_OPT_SINCE_LAST_OPTIMIZATION = "since_last_optimization";
  public static final String F_OPT_LAG_DURATION = "lag_duration";

  // TableSummaryMetrics fields (15 total)
  public static final String F_SUMMARY_TOTAL_FILES = "total_files";
  public static final String F_SUMMARY_DATA_FILES = "data_files";
  public static final String F_SUMMARY_EQUALITY_DELETE_FILES = "equality_delete_files";
  public static final String F_SUMMARY_POSITION_DELETE_FILES = "position_delete_files";
  public static final String F_SUMMARY_DANGLING_DELETE_FILES = "dangling_delete_files";
  public static final String F_SUMMARY_TOTAL_FILES_SIZE = "total_files_size";
  public static final String F_SUMMARY_DATA_FILES_SIZE = "data_files_size";
  public static final String F_SUMMARY_EQUALITY_DELETE_FILES_SIZE = "equality_delete_files_size";
  public static final String F_SUMMARY_POSITION_DELETE_FILES_SIZE = "position_delete_files_size";
  public static final String F_SUMMARY_TOTAL_RECORDS = "total_records";
  public static final String F_SUMMARY_DATA_FILES_RECORDS = "data_files_records";
  public static final String F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS =
      "equality_delete_files_records";
  public static final String F_SUMMARY_POSITION_DELETE_FILES_RECORDS =
      "position_delete_files_records";
  public static final String F_SUMMARY_SNAPSHOTS = "snapshots";
  public static final String F_SUMMARY_HEALTH_SCORE = "health_score";

  // TableOrphanFilesCleaningMetrics fields (4 total)
  public static final String F_ORPHAN_CONTENT_FILE_CLEANING_COUNT =
      "orphan_content_file_cleaning_count";
  public static final String F_ORPHAN_METADATA_FILE_CLEANING_COUNT =
      "orphan_metadata_file_cleaning_count";
  public static final String F_ORPHAN_EXPECTED_CONTENT_FILE_CLEANING_COUNT =
      "expected_orphan_content_file_cleaning_count";
  public static final String F_ORPHAN_EXPECTED_METADATA_FILE_CLEANING_COUNT =
      "expected_orphan_metadata_file_cleaning_count";

  // -------- Pre-baked profile sets (immutable) ---------------------------------------------

  // Group whitelist for the LEGACY profile contains every field — i.e. equivalent to no filter.
  private static final Set<String> LEGACY_GROUP =
      ImmutableSet.of(
          F_GROUP_PENDING_TASKS,
          F_GROUP_EXECUTING_TASKS,
          F_GROUP_PLANING_TABLES,
          F_GROUP_PENDING_TABLES,
          F_GROUP_EXECUTING_TABLES,
          F_GROUP_IDLE_TABLES,
          F_GROUP_COMMITTING_TABLES,
          F_GROUP_OPTIMIZER_INSTANCES,
          F_GROUP_MEMORY_BYTES_ALLOCATED,
          F_GROUP_THREADS);

  private static final Set<String> LEGACY_OPTIMIZING =
      ImmutableSet.of(
          F_OPT_STATUS_IDLE_DURATION,
          F_OPT_STATUS_PENDING_DURATION,
          F_OPT_STATUS_PLANNING_DURATION,
          F_OPT_STATUS_EXECUTING_DURATION,
          F_OPT_STATUS_COMMITTING_DURATION,
          F_OPT_STATUS_IN_IDLE,
          F_OPT_STATUS_IN_PENDING,
          F_OPT_STATUS_IN_PLANNING,
          F_OPT_STATUS_IN_EXECUTING,
          F_OPT_STATUS_IN_COMMITTING,
          F_OPT_PROCESS_TOTAL_COUNT,
          F_OPT_PROCESS_FAILED_COUNT,
          F_OPT_MINOR_TOTAL_COUNT,
          F_OPT_MINOR_FAILED_COUNT,
          F_OPT_MAJOR_TOTAL_COUNT,
          F_OPT_MAJOR_FAILED_COUNT,
          F_OPT_FULL_TOTAL_COUNT,
          F_OPT_FULL_FAILED_COUNT,
          F_OPT_PROCESS_CONSECUTIVE_FAILURES,
          F_OPT_SINCE_LAST_MINOR_OPTIMIZATION,
          F_OPT_SINCE_LAST_MAJOR_OPTIMIZATION,
          F_OPT_SINCE_LAST_FULL_OPTIMIZATION,
          F_OPT_SINCE_LAST_OPTIMIZATION,
          F_OPT_LAG_DURATION);

  private static final Set<String> LEGACY_SUMMARY =
      ImmutableSet.of(
          F_SUMMARY_TOTAL_FILES,
          F_SUMMARY_DATA_FILES,
          F_SUMMARY_EQUALITY_DELETE_FILES,
          F_SUMMARY_POSITION_DELETE_FILES,
          F_SUMMARY_DANGLING_DELETE_FILES,
          F_SUMMARY_TOTAL_FILES_SIZE,
          F_SUMMARY_DATA_FILES_SIZE,
          F_SUMMARY_EQUALITY_DELETE_FILES_SIZE,
          F_SUMMARY_POSITION_DELETE_FILES_SIZE,
          F_SUMMARY_TOTAL_RECORDS,
          F_SUMMARY_DATA_FILES_RECORDS,
          F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS,
          F_SUMMARY_POSITION_DELETE_FILES_RECORDS,
          F_SUMMARY_SNAPSHOTS,
          F_SUMMARY_HEALTH_SCORE);

  private static final Set<String> LEGACY_ORPHAN =
      ImmutableSet.of(
          F_ORPHAN_CONTENT_FILE_CLEANING_COUNT,
          F_ORPHAN_METADATA_FILE_CLEANING_COUNT,
          F_ORPHAN_EXPECTED_CONTENT_FILE_CLEANING_COUNT,
          F_ORPHAN_EXPECTED_METADATA_FILE_CLEANING_COUNT);

  // RECOMMENDED — exactly the 26 KEEP metrics from §3.8 of the design doc.
  private static final Set<String> RECOMMENDED_GROUP =
      ImmutableSet.of(
          F_GROUP_PENDING_TASKS,
          F_GROUP_EXECUTING_TASKS,
          F_GROUP_PENDING_TABLES,
          F_GROUP_EXECUTING_TABLES,
          F_GROUP_COMMITTING_TABLES,
          F_GROUP_OPTIMIZER_INSTANCES,
          F_GROUP_MEMORY_BYTES_ALLOCATED,
          F_GROUP_THREADS);

  // RECOMMENDED — under the value-based filter (`metric.optimizing.skip-zero-status` +
  // per-state thresholds) the 5 `status_*_duration_mills` gauges strictly subsume the 5
  // `status_in_*` indicators (`duration > 0 ⇔ in_state == 1`) and additionally carry the
  // "how long" magnitude required by SLO/alert thresholds. Therefore RECOMMENDED whitelists
  // the 5 durations and drops all 5 indicators. The actual emitted series count is further
  // suppressed at runtime by the value/threshold-based dynamic registration. See design doc
  // §2.3.1 / §3.3 (Revision 2026-06-02).
  // NOTE: F_OPT_STATUS_IDLE_DURATION is intentionally excluded — `idle` is the steady-state
  // for a healthy table (no work pending), so emitting per-table idle duration carries no
  // alarm signal and only inflates per-table cardinality. The other 4 durations describe
  // active stages that may get stuck and are therefore retained. Operators who still need
  // an idle gauge can opt back via LEGACY or CUSTOM. lag_duration is also dropped because
  // operators can monitor data freshness via the group-level `optimizer_group_pending_tables`
  // queue and per-table failure via `process_failed_count`; lag_duration remains available
  // under LEGACY / CUSTOM. See design doc §2.3.1 (Revision 2026-06-02 follow-up).
  private static final Set<String> RECOMMENDED_OPTIMIZING =
      ImmutableSet.of(
          F_OPT_STATUS_PENDING_DURATION,
          F_OPT_STATUS_PLANNING_DURATION,
          F_OPT_STATUS_EXECUTING_DURATION,
          F_OPT_STATUS_COMMITTING_DURATION,
          F_OPT_PROCESS_TOTAL_COUNT,
          F_OPT_PROCESS_FAILED_COUNT,
          F_OPT_PROCESS_CONSECUTIVE_FAILURES,
          F_OPT_SINCE_LAST_OPTIMIZATION);

  // Group U (summary) — RECOMMENDED keeps the 7 delete-related signals plus the aggregated
  // health_score. Capacity attributes (total/data files, sizes, records, snapshots) are
  // dropped because they are obtainable via REST API / HMS / file system listing and do not
  // drive optimize-trigger or read-amplification alarms. Only delete files / sizes / records
  // and the AMS-computed health_score directly reflect read-amplification and the urgency of
  // optimize. See design doc §3.6 (Revision 2026-06-01).
  private static final Set<String> RECOMMENDED_SUMMARY =
      ImmutableSet.of(
          F_SUMMARY_EQUALITY_DELETE_FILES,
          F_SUMMARY_POSITION_DELETE_FILES,
          F_SUMMARY_DANGLING_DELETE_FILES,
          F_SUMMARY_EQUALITY_DELETE_FILES_SIZE,
          F_SUMMARY_POSITION_DELETE_FILES_SIZE,
          F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS,
          F_SUMMARY_POSITION_DELETE_FILES_RECORDS,
          F_SUMMARY_HEALTH_SCORE);

  // Group V (orphan) is intentionally empty in RECOMMENDED:
  // orphan-files cleaning runs at most once per day; the same expected/deleted numbers are
  // already exposed as INFO logs ("Deleted {n}/{m} ... files for table {}") in
  // IcebergTableMaintainer, so the counters add cardinality without additional signal.
  // Only LEGACY (and CUSTOM, on demand) keep them registered.
  private static final Set<String> RECOMMENDED_ORPHAN = ImmutableSet.of();

  // MINIMAL — 12 metrics for very high-cardinality deployments (§4.4 of the design doc).
  private static final Set<String> MINIMAL_GROUP =
      ImmutableSet.of(
          F_GROUP_PENDING_TASKS,
          F_GROUP_EXECUTING_TASKS,
          F_GROUP_PENDING_TABLES,
          F_GROUP_EXECUTING_TABLES,
          F_GROUP_OPTIMIZER_INSTANCES);

  // MINIMAL — same status-class shape as RECOMMENDED (5 durations under value-based filter,
  // 0 indicators) but drops `process_total_count`, `since_last_optimization` and
  // `lag_duration` on top of RECOMMENDED to push per-table cardinality further down for
  // ≥100k-table deployments. The freshness/lag gauges are not action-driving at minimal-tier
  // observability — operators monitor freshness via the group-level
  // `optimizer_group_pending_tables` queue and per-table failure via `process_failed_count`.
  // The 5 durations are still gated by `metric.optimizing.skip-zero-status` + per-state
  // thresholds at runtime, so most healthy tables emit 0 status series. See design doc
  // §2.3.1 / §3.3 (Revision 2026-06-02).
  // NOTE: F_OPT_STATUS_IDLE_DURATION is excluded for the same reason as in
  // RECOMMENDED_OPTIMIZING — it is the steady-state of a healthy table.
  private static final Set<String> MINIMAL_OPTIMIZING =
      ImmutableSet.of(
          F_OPT_STATUS_PENDING_DURATION,
          F_OPT_STATUS_PLANNING_DURATION,
          F_OPT_STATUS_EXECUTING_DURATION,
          F_OPT_STATUS_COMMITTING_DURATION,
          F_OPT_PROCESS_FAILED_COUNT,
          F_OPT_PROCESS_CONSECUTIVE_FAILURES);

  // MINIMAL trims the aggregated health_score on top of RECOMMENDED, leaving only the 7
  // raw delete-related signals. Operators who need a one-glance health summary should use
  // RECOMMENDED; MINIMAL is intended for the largest deployments where every per-table
  // gauge counts.
  private static final Set<String> MINIMAL_SUMMARY =
      ImmutableSet.of(
          F_SUMMARY_EQUALITY_DELETE_FILES,
          F_SUMMARY_POSITION_DELETE_FILES,
          F_SUMMARY_DANGLING_DELETE_FILES,
          F_SUMMARY_EQUALITY_DELETE_FILES_SIZE,
          F_SUMMARY_POSITION_DELETE_FILES_SIZE,
          F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS,
          F_SUMMARY_POSITION_DELETE_FILES_RECORDS);

  private static final Set<String> MINIMAL_ORPHAN = ImmutableSet.of();

  // -------- Singleton + state --------------------------------------------------------------

  private static volatile MetricFilterConfig instance = legacyDefault();

  private final Profile profile;
  private final Set<String> optimizingFields;
  private final Set<String> summaryFields;
  private final Set<String> orphanFields;
  private final Set<String> groupFields;
  private final boolean skipZeroDeletes;
  private final boolean skipZeroStatus;
  private final Map<String, Long> statusThresholdByState;

  MetricFilterConfig(
      Profile profile,
      Set<String> optimizingFields,
      Set<String> summaryFields,
      Set<String> orphanFields,
      Set<String> groupFields) {
    this(
        profile,
        optimizingFields,
        summaryFields,
        orphanFields,
        groupFields,
        false,
        false,
        Collections.emptyMap());
  }

  MetricFilterConfig(
      Profile profile,
      Set<String> optimizingFields,
      Set<String> summaryFields,
      Set<String> orphanFields,
      Set<String> groupFields,
      boolean skipZeroDeletes) {
    this(
        profile,
        optimizingFields,
        summaryFields,
        orphanFields,
        groupFields,
        skipZeroDeletes,
        false,
        Collections.emptyMap());
  }

  MetricFilterConfig(
      Profile profile,
      Set<String> optimizingFields,
      Set<String> summaryFields,
      Set<String> orphanFields,
      Set<String> groupFields,
      boolean skipZeroDeletes,
      boolean skipZeroStatus,
      Map<String, Long> statusThresholdByState) {
    this.profile = Objects.requireNonNull(profile);
    this.optimizingFields = Collections.unmodifiableSet(new HashSet<>(optimizingFields));
    this.summaryFields = Collections.unmodifiableSet(new HashSet<>(summaryFields));
    this.orphanFields = Collections.unmodifiableSet(new HashSet<>(orphanFields));
    this.groupFields = Collections.unmodifiableSet(new HashSet<>(groupFields));
    this.skipZeroDeletes = skipZeroDeletes;
    this.skipZeroStatus = skipZeroStatus;
    this.statusThresholdByState =
        Collections.unmodifiableMap(
            new HashMap<>(
                statusThresholdByState == null ? Collections.emptyMap() : statusThresholdByState));
  }

  /** Build the default LEGACY filter; used when {@link #init(Configurations)} has not run. */
  private static MetricFilterConfig legacyDefault() {
    return new MetricFilterConfig(
        Profile.LEGACY, LEGACY_OPTIMIZING, LEGACY_SUMMARY, LEGACY_ORPHAN, LEGACY_GROUP);
  }

  /**
   * Build a filter from the given {@code Configurations} and install it as the singleton. Must be
   * called exactly once at AMS bootstrap before any metric class is instantiated.
   *
   * @throws IllegalArgumentException if {@code metric.profile} is invalid.
   */
  public static synchronized void init(Configurations conf) {
    Objects.requireNonNull(conf, "Configurations must not be null");
    Profile profile = Profile.parse(conf.getString(AmoroManagementConf.METRIC_PROFILE));
    instance = build(profile, conf);
    LOG.info(
        "Metric filter installed: profile={} optimizing={} summary={} orphan={} group={} "
            + "skipZeroDeletes={} skipZeroStatus={} statusThresholdByState={}",
        instance.profile,
        instance.optimizingFields.size(),
        instance.summaryFields.size(),
        instance.orphanFields.size(),
        instance.groupFields.size(),
        instance.skipZeroDeletes,
        instance.skipZeroStatus,
        instance.statusThresholdByState);
  }

  /** Test-only / programmatic override. */
  public static synchronized void install(MetricFilterConfig config) {
    instance = Objects.requireNonNull(config);
  }

  /** Reset to the legacy default. Mostly for tests. */
  public static synchronized void reset() {
    instance = legacyDefault();
  }

  /** Return the current filter; never null. */
  public static MetricFilterConfig current() {
    return instance;
  }

  static MetricFilterConfig build(Profile profile, Configurations conf) {
    boolean skipZeroDeletes =
        conf != null && conf.getBoolean(AmoroManagementConf.METRIC_SUMMARY_SKIP_ZERO_DELETES);
    boolean skipZeroStatus =
        conf != null && conf.getBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS);
    Map<String, Long> thresholds = readStatusThresholds(conf);
    switch (profile) {
      case RECOMMENDED:
        return new MetricFilterConfig(
            profile,
            RECOMMENDED_OPTIMIZING,
            RECOMMENDED_SUMMARY,
            RECOMMENDED_ORPHAN,
            RECOMMENDED_GROUP,
            skipZeroDeletes,
            skipZeroStatus,
            thresholds);
      case MINIMAL:
        return new MetricFilterConfig(
            profile,
            MINIMAL_OPTIMIZING,
            MINIMAL_SUMMARY,
            MINIMAL_ORPHAN,
            MINIMAL_GROUP,
            skipZeroDeletes,
            skipZeroStatus,
            thresholds);
      case LEGACY:
        return new MetricFilterConfig(
            profile,
            LEGACY_OPTIMIZING,
            LEGACY_SUMMARY,
            LEGACY_ORPHAN,
            LEGACY_GROUP,
            skipZeroDeletes,
            skipZeroStatus,
            thresholds);
      case CUSTOM:
        return buildCustom(conf, skipZeroDeletes, skipZeroStatus, thresholds);
      default:
        // Unreachable: Profile.parse() guards against unknown values.
        throw new IllegalStateException("Unhandled profile: " + profile);
    }
  }

  private static MetricFilterConfig buildCustom(
      Configurations conf,
      boolean skipZeroDeletes,
      boolean skipZeroStatus,
      Map<String, Long> thresholds) {
    return new MetricFilterConfig(
        Profile.CUSTOM,
        toSet(conf.get(AmoroManagementConf.METRIC_OPTIMIZING_FIELDS)),
        toSet(conf.get(AmoroManagementConf.METRIC_SUMMARY_FIELDS)),
        toSet(conf.get(AmoroManagementConf.METRIC_ORPHAN_FIELDS)),
        toSet(conf.get(AmoroManagementConf.METRIC_OPTIMIZER_GROUP_FIELDS)),
        skipZeroDeletes,
        skipZeroStatus,
        thresholds);
  }

  /**
   * Read the five per-state independent duration thresholds from the configuration. Negative values
   * are coerced to 0 with a WARN log; entries with the default value 0 are still kept in the map so
   * that {@link #effectiveStatusThreshold(String)} can short-circuit via {@code getOrDefault}.
   */
  private static Map<String, Long> readStatusThresholds(Configurations conf) {
    if (conf == null) {
      return Collections.emptyMap();
    }
    Map<String, Long> out = new HashMap<>(5);
    putThreshold(
        out,
        TableOptimizingMetrics.STATUS_IDLE,
        conf.getLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_IDLE),
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_IDLE.key());
    putThreshold(
        out,
        TableOptimizingMetrics.STATUS_PENDING,
        conf.getLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING),
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING.key());
    putThreshold(
        out,
        TableOptimizingMetrics.STATUS_PLANING,
        conf.getLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PLANNING),
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PLANNING.key());
    putThreshold(
        out,
        TableOptimizingMetrics.STATUS_EXECUTING,
        conf.getLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_EXECUTING),
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_EXECUTING.key());
    putThreshold(
        out,
        TableOptimizingMetrics.STATUS_COMMITTING,
        conf.getLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_COMMITTING),
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_COMMITTING.key());
    return ImmutableMap.copyOf(out);
  }

  private static void putThreshold(Map<String, Long> sink, String state, long raw, String key) {
    long sanitized = raw;
    if (raw < 0L) {
      LOG.warn("Negative threshold {}={} coerced to 0", key, raw);
      sanitized = 0L;
    }
    sink.put(state, sanitized);
  }

  private static Set<String> toSet(List<String> raw) {
    if (raw == null || raw.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> out = new HashSet<>(raw.size());
    for (String s : raw) {
      if (s != null) {
        String trimmed = s.trim();
        if (!trimmed.isEmpty()) {
          out.add(trimmed);
        }
      }
    }
    return out;
  }

  // -------- Public allow-* API -------------------------------------------------------------

  public Profile profile() {
    return profile;
  }

  public boolean allowOptimizing(String field) {
    return optimizingFields.contains(field);
  }

  public boolean allowSummary(String field) {
    return summaryFields.contains(field);
  }

  public boolean allowOrphan(String field) {
    return orphanFields.contains(field);
  }

  public boolean allowGroup(String field) {
    return groupFields.contains(field);
  }

  /**
   * Whether to register the seven delete-related TableSummaryMetrics fields only while their
   * current value is > 0. Each metric is judged independently against its own value (no joint
   * condition). When false (default) all whitelisted delete fields are registered eagerly,
   * preserving the upstream community behaviour. This switch is orthogonal to {@link Profile}.
   */
  public boolean isSkipZeroDeletes() {
    return skipZeroDeletes;
  }

  /**
   * Whether the 10 status-class TableOptimizingMetrics fields (5 status_in_* + 5
   * status_*_duration_mills) are subject to dynamic value/threshold-based registration. When false
   * (default) those fields are eagerly registered at table bootstrap, preserving the upstream
   * community behaviour. Orthogonal to {@link Profile}.
   */
  public boolean isSkipZeroStatus() {
    return skipZeroStatus;
  }

  /**
   * Resolve the effective duration threshold (millis) for the given optimizing state. The state is
   * one of the five string constants exposed by {@code TableOptimizingMetrics} (idle / pending /
   * planing / executing / committing). When the per-state config option is absent the default 0 is
   * returned (any positive duration triggers exposure).
   */
  public long effectiveStatusThreshold(String state) {
    return statusThresholdByState.getOrDefault(state, 0L);
  }
}
