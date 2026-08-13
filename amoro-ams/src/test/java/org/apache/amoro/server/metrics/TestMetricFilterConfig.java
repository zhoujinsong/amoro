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

import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_COMMITTING_TABLES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_EXECUTING_TABLES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_EXECUTING_TASKS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_IDLE_TABLES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_MEMORY_BYTES_ALLOCATED;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_OPTIMIZER_INSTANCES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_PENDING_TABLES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_PENDING_TASKS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_PLANING_TABLES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_GROUP_THREADS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_LAG_DURATION;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_PROCESS_FAILED_COUNT;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_PROCESS_TOTAL_COUNT;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_SINCE_LAST_OPTIMIZATION;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_STATUS_COMMITTING_DURATION;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_STATUS_EXECUTING_DURATION;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_STATUS_IDLE_DURATION;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_STATUS_IN_EXECUTING;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_STATUS_IN_PENDING;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_STATUS_PENDING_DURATION;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_OPT_STATUS_PLANNING_DURATION;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_ORPHAN_CONTENT_FILE_CLEANING_COUNT;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_ORPHAN_EXPECTED_CONTENT_FILE_CLEANING_COUNT;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_ORPHAN_EXPECTED_METADATA_FILE_CLEANING_COUNT;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_ORPHAN_METADATA_FILE_CLEANING_COUNT;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_DANGLING_DELETE_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_DATA_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_DATA_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_EQUALITY_DELETE_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_EQUALITY_DELETE_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_HEALTH_SCORE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_POSITION_DELETE_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_POSITION_DELETE_FILES_RECORDS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_POSITION_DELETE_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_SNAPSHOTS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_TOTAL_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_TOTAL_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_TOTAL_RECORDS;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Unit tests for {@link MetricFilterConfig}.
 *
 * <p>Verifies that each pre-baked profile (LEGACY / RECOMMENDED / MINIMAL) admits exactly the
 * cardinality stated in the design document {@code 2026-05-29-AMS指标体系精简.md}, and that the CUSTOM
 * profile honours the four {@code metric.*.fields} lists verbatim. Together these cases cover the
 * four scenarios required by acceptance checklist §7.3.
 */
public class TestMetricFilterConfig {

  @AfterEach
  public void tearDown() {
    // Restore the global default so no test leaks state into the next test class.
    MetricFilterConfig.reset();
  }

  // --------------------------------------------------------------------------------------------
  // Profile parsing
  // --------------------------------------------------------------------------------------------

  @Test
  public void testProfileParseDefaultsToLegacyOnNull() {
    Configurations conf = new Configurations();
    MetricFilterConfig.init(conf);
    Assertions.assertEquals(
        MetricFilterConfig.Profile.LEGACY, MetricFilterConfig.current().profile());
  }

  @Test
  public void testProfileParseIsCaseInsensitive() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "Recommended");
    MetricFilterConfig.init(conf);
    Assertions.assertEquals(
        MetricFilterConfig.Profile.RECOMMENDED, MetricFilterConfig.current().profile());
  }

  @Test
  public void testProfileParseRejectsUnknownValue() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "bogus");
    Assertions.assertThrows(IllegalArgumentException.class, () -> MetricFilterConfig.init(conf));
  }

  // --------------------------------------------------------------------------------------------
  // LEGACY profile — must accept every field from §2 (cardinality 60)
  // --------------------------------------------------------------------------------------------

  @Test
  public void testLegacyProfileAcceptsAllFields() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Group: 10 fields
    Assertions.assertTrue(f.allowGroup(F_GROUP_PENDING_TASKS));
    Assertions.assertTrue(f.allowGroup(F_GROUP_EXECUTING_TASKS));
    Assertions.assertTrue(f.allowGroup(F_GROUP_PLANING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_PENDING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_EXECUTING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_IDLE_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_COMMITTING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_OPTIMIZER_INSTANCES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_MEMORY_BYTES_ALLOCATED));
    Assertions.assertTrue(f.allowGroup(F_GROUP_THREADS));

    // Spot-check optimizing / summary / orphan: legacy must accept fields the recommended
    // profile drops.
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_IDLE_DURATION));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_TOTAL_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_DATA_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_SNAPSHOTS));
    Assertions.assertTrue(f.allowOrphan(F_ORPHAN_EXPECTED_CONTENT_FILE_CLEANING_COUNT));
    Assertions.assertTrue(f.allowOrphan(F_ORPHAN_EXPECTED_METADATA_FILE_CLEANING_COUNT));
  }

  // --------------------------------------------------------------------------------------------
  // RECOMMENDED profile (§3.8)
  // --------------------------------------------------------------------------------------------

  @Test
  public void testRecommendedProfileGroup() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Accepted (8): pending_tasks, executing_tasks, pending_tables, executing_tables,
    // committing_tables, optimizer_instances, memory_bytes_allocated, threads.
    Assertions.assertTrue(f.allowGroup(F_GROUP_PENDING_TASKS));
    Assertions.assertTrue(f.allowGroup(F_GROUP_EXECUTING_TASKS));
    Assertions.assertTrue(f.allowGroup(F_GROUP_PENDING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_EXECUTING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_COMMITTING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_OPTIMIZER_INSTANCES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_MEMORY_BYTES_ALLOCATED));
    Assertions.assertTrue(f.allowGroup(F_GROUP_THREADS));

    // Dropped (2): planing_tables, idle_tables.
    Assertions.assertFalse(f.allowGroup(F_GROUP_PLANING_TABLES));
    Assertions.assertFalse(f.allowGroup(F_GROUP_IDLE_TABLES));
  }

  @Test
  public void testRecommendedProfileOptimizing() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Accepted (7): the 4 active-stage status_*_duration_mills gauges (gated by
    // skip-zero-status + per-state thresholds at runtime), process_total, process_failed,
    // since_last_optimization. The 5 status_in_* indicators are dropped
    // because they are strictly subsumed by the duration gauges (see design doc
    // §2.3.1 / §3.3, Revision 2026-06-02). status_idle_duration is also dropped because
    // `idle` is the steady-state of a healthy table and carries no alarm signal
    // (Revision 2026-06-02 follow-up). lag_duration is also dropped from `recommended`
    // because operators can monitor data freshness via the group-level
    // `optimizer_group_pending_tables` queue; lag_duration remains available under
    // `legacy` / `custom`.
    Assertions.assertFalse(f.allowOptimizing(F_OPT_STATUS_IDLE_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_PENDING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_PLANNING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_EXECUTING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_COMMITTING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_PROCESS_TOTAL_COUNT));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_PROCESS_FAILED_COUNT));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_SINCE_LAST_OPTIMIZATION));
    Assertions.assertFalse(f.allowOptimizing(F_OPT_LAG_DURATION));

    // Dropped: all 5 status_in_* indicators (subsumed by durations), per-type counters,
    // per-type since_last_*.
    Assertions.assertFalse(f.allowOptimizing(F_OPT_STATUS_IN_PENDING));
    Assertions.assertFalse(f.allowOptimizing(F_OPT_STATUS_IN_EXECUTING));
  }

  @Test
  public void testRecommendedProfileSummary() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Accepted (8): the 7 raw delete-related signals plus the aggregated health_score.
    // Rationale (§3.6 of the design doc, Revision 2026-06-01): deletes are the key driver
    // of read amplification and optimize-trigger; capacity attributes are obtainable via
    // REST API / HMS and therefore not registered as metrics in the recommended profile.
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_DANGLING_DELETE_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_EQUALITY_DELETE_FILES_SIZE));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_POSITION_DELETE_FILES_SIZE));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_POSITION_DELETE_FILES_RECORDS));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_HEALTH_SCORE));

    // Dropped (7): capacity attributes — total/data files, sizes, records and snapshots.
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_FILES));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_DATA_FILES));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_FILES_SIZE));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_DATA_FILES_SIZE));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_RECORDS));
    Assertions.assertFalse(f.allowSummary("data_files_records"));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_SNAPSHOTS));
  }

  @Test
  public void testRecommendedProfileOrphanIsEmpty() {
    // Group V (orphan) was trimmed from the recommended profile on 2026-06-01: the orphan
    // cleaning task runs at most once per day and the same expected/deleted numbers are already
    // exposed as INFO logs by IcebergTableMaintainer ("Deleted {n}/{m} ... files for table {}").
    // Only LEGACY keeps these counters registered by default; CUSTOM can opt in explicitly.
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_CONTENT_FILE_CLEANING_COUNT));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_METADATA_FILE_CLEANING_COUNT));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_EXPECTED_CONTENT_FILE_CLEANING_COUNT));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_EXPECTED_METADATA_FILE_CLEANING_COUNT));
  }

  // --------------------------------------------------------------------------------------------
  // MINIMAL profile (§4.4)
  // --------------------------------------------------------------------------------------------

  @Test
  public void testMinimalProfileGroup() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "minimal");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Accepted (5).
    Assertions.assertTrue(f.allowGroup(F_GROUP_PENDING_TASKS));
    Assertions.assertTrue(f.allowGroup(F_GROUP_EXECUTING_TASKS));
    Assertions.assertTrue(f.allowGroup(F_GROUP_PENDING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_EXECUTING_TABLES));
    Assertions.assertTrue(f.allowGroup(F_GROUP_OPTIMIZER_INSTANCES));

    // Dropped: committing_tables, memory_bytes_allocated, threads, planing_tables, idle_tables.
    Assertions.assertFalse(f.allowGroup(F_GROUP_COMMITTING_TABLES));
    Assertions.assertFalse(f.allowGroup(F_GROUP_MEMORY_BYTES_ALLOCATED));
    Assertions.assertFalse(f.allowGroup(F_GROUP_THREADS));
  }

  @Test
  public void testMinimalProfileOptimizing() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "minimal");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Accepted (5): the 4 active-stage status_*_duration_mills gauges (gated by
    // skip-zero-status + per-state thresholds at runtime) plus process_failed_count.
    // The 5 status_in_* indicators are dropped because they are strictly subsumed by
    // the duration gauges (see design doc §2.3.1 / §3.3, Revision 2026-06-02).
    // status_idle_duration is also dropped because `idle` is the steady-state of a
    // healthy table and carries no alarm signal (Revision 2026-06-02 follow-up).
    Assertions.assertFalse(f.allowOptimizing(F_OPT_STATUS_IDLE_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_PENDING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_PLANNING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_EXECUTING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_STATUS_COMMITTING_DURATION));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_PROCESS_FAILED_COUNT));

    // Dropped: all 5 status_in_* indicators (subsumed by durations).
    Assertions.assertFalse(f.allowOptimizing(F_OPT_STATUS_IN_PENDING));
    Assertions.assertFalse(f.allowOptimizing(F_OPT_STATUS_IN_EXECUTING));
    // Dropped: process_total_count is recommended-only, not minimal.
    Assertions.assertFalse(f.allowOptimizing(F_OPT_PROCESS_TOTAL_COUNT));
    // Dropped on top of RECOMMENDED: freshness/lag gauges are dropped from minimal
    // because per-table cardinality dominates in the largest deployments and these
    // gauges are not action-driving at minimal-tier observability.
    Assertions.assertFalse(f.allowOptimizing(F_OPT_SINCE_LAST_OPTIMIZATION));
    Assertions.assertFalse(f.allowOptimizing(F_OPT_LAG_DURATION));
  }

  @Test
  public void testMinimalProfileSummary() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "minimal");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Accepted (7): only the raw delete-related signals. health_score is dropped on top of
    // RECOMMENDED so that the very largest deployments minimise per-table cardinality.
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_DANGLING_DELETE_FILES));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_EQUALITY_DELETE_FILES_SIZE));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_POSITION_DELETE_FILES_SIZE));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_POSITION_DELETE_FILES_RECORDS));

    // Dropped (8): capacity attributes plus health_score (recommended-only).
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_FILES));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_DATA_FILES));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_FILES_SIZE));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_DATA_FILES_SIZE));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_RECORDS));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_SNAPSHOTS));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_HEALTH_SCORE));
  }

  @Test
  public void testMinimalProfileOrphanIsEmpty() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "minimal");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_CONTENT_FILE_CLEANING_COUNT));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_METADATA_FILE_CLEANING_COUNT));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_EXPECTED_CONTENT_FILE_CLEANING_COUNT));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_EXPECTED_METADATA_FILE_CLEANING_COUNT));
  }

  // --------------------------------------------------------------------------------------------
  // CUSTOM profile — honours the four lists verbatim
  // --------------------------------------------------------------------------------------------

  @Test
  public void testCustomProfileEmptyListsRegisterNothing() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "custom");
    // No fields configured -> all four lists are empty -> nothing is allowed.
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertFalse(f.allowGroup(F_GROUP_PENDING_TASKS));
    Assertions.assertFalse(f.allowOptimizing(F_OPT_LAG_DURATION));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_FILES));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_CONTENT_FILE_CLEANING_COUNT));
  }

  @Test
  public void testCustomProfileWithExplicitLists() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "custom");
    conf.set(
        AmoroManagementConf.METRIC_OPTIMIZER_GROUP_FIELDS,
        Arrays.asList(F_GROUP_PENDING_TASKS, F_GROUP_EXECUTING_TASKS));
    conf.set(
        AmoroManagementConf.METRIC_OPTIMIZING_FIELDS,
        Collections.singletonList(F_OPT_LAG_DURATION));
    conf.set(
        AmoroManagementConf.METRIC_SUMMARY_FIELDS,
        Collections.singletonList(F_SUMMARY_HEALTH_SCORE));
    conf.set(
        AmoroManagementConf.METRIC_ORPHAN_FIELDS,
        Collections.singletonList(F_ORPHAN_CONTENT_FILE_CLEANING_COUNT));

    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    // Configured fields are accepted.
    Assertions.assertTrue(f.allowGroup(F_GROUP_PENDING_TASKS));
    Assertions.assertTrue(f.allowGroup(F_GROUP_EXECUTING_TASKS));
    Assertions.assertTrue(f.allowOptimizing(F_OPT_LAG_DURATION));
    Assertions.assertTrue(f.allowSummary(F_SUMMARY_HEALTH_SCORE));
    Assertions.assertTrue(f.allowOrphan(F_ORPHAN_CONTENT_FILE_CLEANING_COUNT));

    // Anything not in the configured lists is rejected.
    Assertions.assertFalse(f.allowGroup(F_GROUP_PENDING_TABLES));
    Assertions.assertFalse(f.allowOptimizing(F_OPT_PROCESS_TOTAL_COUNT));
    Assertions.assertFalse(f.allowSummary(F_SUMMARY_TOTAL_FILES));
    Assertions.assertFalse(f.allowOrphan(F_ORPHAN_METADATA_FILE_CLEANING_COUNT));
  }

  @Test
  public void testCustomProfileTrimsAndIgnoresEmptyEntries() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "custom");
    // Whitespace and blank entries must be tolerated.
    conf.set(
        AmoroManagementConf.METRIC_OPTIMIZING_FIELDS,
        Arrays.asList("  " + F_OPT_LAG_DURATION + "  ", "", " "));

    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertTrue(f.allowOptimizing(F_OPT_LAG_DURATION));
    Assertions.assertFalse(f.allowOptimizing(F_OPT_PROCESS_TOTAL_COUNT));
  }

  @Test
  public void testCustomProfileUnknownFieldIsTreatedAsOpaque() {
    // Unknown field names are simply never matched by allow* — no exception is thrown.
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "custom");
    conf.set(
        AmoroManagementConf.METRIC_SUMMARY_FIELDS,
        Arrays.asList("not_a_real_field", F_SUMMARY_TOTAL_FILES));

    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertTrue(f.allowSummary(F_SUMMARY_TOTAL_FILES));
    Assertions.assertFalse(f.allowSummary("not_a_real_field_2"));
  }

  // --------------------------------------------------------------------------------------------
  // Singleton lifecycle
  // --------------------------------------------------------------------------------------------

  @Test
  public void testResetReturnsLegacyDefault() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "minimal");
    MetricFilterConfig.init(conf);
    Assertions.assertEquals(
        MetricFilterConfig.Profile.MINIMAL, MetricFilterConfig.current().profile());

    MetricFilterConfig.reset();
    Assertions.assertEquals(
        MetricFilterConfig.Profile.LEGACY, MetricFilterConfig.current().profile());
  }

  @Test
  public void testCurrentNeverReturnsNullBeforeInit() {
    MetricFilterConfig.reset();
    Assertions.assertNotNull(MetricFilterConfig.current());
    Assertions.assertEquals(
        MetricFilterConfig.Profile.LEGACY, MetricFilterConfig.current().profile());
  }

  // --------------------------------------------------------------------------------------------
  // metric.summary.skip-zero-deletes — global value-based filter switch
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSkipZeroDeletesDefaultFalse() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    MetricFilterConfig.init(conf);
    Assertions.assertFalse(MetricFilterConfig.current().isSkipZeroDeletes());
  }

  @Test
  public void testSkipZeroDeletesExplicitTrue() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    conf.setBoolean(AmoroManagementConf.METRIC_SUMMARY_SKIP_ZERO_DELETES, true);
    MetricFilterConfig.init(conf);
    Assertions.assertTrue(MetricFilterConfig.current().isSkipZeroDeletes());
  }

  @Test
  public void testSkipZeroDeletesExplicitFalse() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_SUMMARY_SKIP_ZERO_DELETES, false);
    MetricFilterConfig.init(conf);
    Assertions.assertFalse(MetricFilterConfig.current().isSkipZeroDeletes());
  }

  @Test
  public void testSkipZeroDeletesIsOrthogonalToProfile() {
    // The switch must work for every profile, since it is documented as an orthogonal knob.
    for (String profile : new String[] {"recommended", "legacy", "minimal", "custom"}) {
      Configurations conf = new Configurations();
      conf.setString(AmoroManagementConf.METRIC_PROFILE, profile);
      conf.setBoolean(AmoroManagementConf.METRIC_SUMMARY_SKIP_ZERO_DELETES, true);
      MetricFilterConfig.init(conf);
      Assertions.assertTrue(
          MetricFilterConfig.current().isSkipZeroDeletes(),
          "skipZeroDeletes must propagate under profile=" + profile);
    }
  }

  // --------------------------------------------------------------------------------------------
  // metric.optimizing.skip-zero-status / status-duration-threshold-mills.* — T4-第一组
  // (设计文档 §2.3, T4 测试列表 t1~t4)
  // --------------------------------------------------------------------------------------------

  /**
   * t1: 默认情况 — skip-zero-status 缺省为 false，5 个 per-state 阈值缺省为 0； effectiveStatusThreshold 对所有 5
   * 个状态都返回 0（二级回退命中 0）。
   */
  @Test
  public void testSkipZeroStatusDefaults() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertFalse(f.isSkipZeroStatus());
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("idle"));
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("pending"));
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("planing"));
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("executing"));
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("committing"));
  }

  /** t2: 5 个 per-state 阈值全配置为推荐值 — effectiveStatusThreshold 必须按状态返回独立阈值， 不存在跨状态串扰。 */
  @Test
  public void testStatusDurationThresholdAllStatesConfigured() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, true);
    conf.setLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_IDLE, 100L);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING, 1_800_000L);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PLANNING, 300_000L);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_EXECUTING, 3_600_000L);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_COMMITTING, 600_000L);
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertTrue(f.isSkipZeroStatus());
    Assertions.assertEquals(100L, f.effectiveStatusThreshold("idle"));
    Assertions.assertEquals(1_800_000L, f.effectiveStatusThreshold("pending"));
    Assertions.assertEquals(300_000L, f.effectiveStatusThreshold("planing"));
    Assertions.assertEquals(3_600_000L, f.effectiveStatusThreshold("executing"));
    Assertions.assertEquals(600_000L, f.effectiveStatusThreshold("committing"));
  }

  /** t3: 部分配置 — 仅显式给 PENDING / EXECUTING 配阈值，其余 3 个状态走默认 0；二级回退正确。 */
  @Test
  public void testStatusDurationThresholdPartialConfiguration() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, true);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING, 1_800_000L);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_EXECUTING, 3_600_000L);
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertTrue(f.isSkipZeroStatus());
    // explicit
    Assertions.assertEquals(1_800_000L, f.effectiveStatusThreshold("pending"));
    Assertions.assertEquals(3_600_000L, f.effectiveStatusThreshold("executing"));
    // default fallback
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("idle"));
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("planing"));
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("committing"));
  }

  /** t4: 非法值（< 0）启动期被强制为 0（设计文档 §2.3.1 与 §5.R7）。同时也验证 unknown state 字符串走默认 0（防御性，不会抛 NPE）。 */
  @Test
  public void testStatusDurationThresholdNegativeValueCoercedToZero() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "recommended");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, true);
    conf.setLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING, -1L);
    conf.setLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_IDLE, -999L);
    MetricFilterConfig.init(conf);
    MetricFilterConfig f = MetricFilterConfig.current();

    Assertions.assertEquals(0L, f.effectiveStatusThreshold("pending"));
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("idle"));
    // unknown state — must not throw, must fall back to 0.
    Assertions.assertEquals(0L, f.effectiveStatusThreshold("unknown_state"));
  }

  /** 补充：skip-zero-status 与 metric.profile 完全正交（与 skip-zero-deletes 对称行为）。 */
  @Test
  public void testSkipZeroStatusIsOrthogonalToProfile() {
    for (String profile : new String[] {"recommended", "legacy", "minimal", "custom"}) {
      Configurations conf = new Configurations();
      conf.setString(AmoroManagementConf.METRIC_PROFILE, profile);
      conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, true);
      MetricFilterConfig.init(conf);
      Assertions.assertTrue(
          MetricFilterConfig.current().isSkipZeroStatus(),
          "skipZeroStatus must propagate under profile=" + profile);
    }
  }
}
