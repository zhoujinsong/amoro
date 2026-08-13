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

package org.apache.amoro.server.table;

import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_COMMITTING_DURATION;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_EXECUTING_DURATION;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_IDLE_DURATION;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_IN_COMMITTING;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_IN_EXECUTING;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_IN_IDLE;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_IN_PENDING;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_IN_PLANNING;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_PENDING_DURATION;
import static org.apache.amoro.server.table.TableOptimizingMetrics.TABLE_OPTIMIZING_STATUS_PLANNING_DURATION;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.metrics.MetricFilterConfig;
import org.apache.amoro.server.metrics.MetricRegistry;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code metric.optimizing.skip-zero-status} value/threshold-based filter
 * implemented in {@link TableOptimizingMetrics}.
 *
 * <p>Covers cases t5~t13 from the design document {@code 2026-06-01-TableOptimizing指标精简方案.md} §4
 * T4. Each case installs a {@link MetricFilterConfig} with the desired switches/thresholds and
 * asserts the registered set on a fresh {@link MetricRegistry}. The 10 covered fields are the 5
 * {@code status_in_*} and 5 {@code status_*_duration_mills} gauges.
 */
public class TestTableOptimizingMetricsSkipZeroStatus {

  private static final ServerTableIdentifier TABLE =
      ServerTableIdentifier.of(1L, "cat", "db", "tbl", TableFormat.ICEBERG);

  private static final String OPTIMIZER_GROUP = "default";

  private static final ImmutableMap<String, String> TAGS =
      ImmutableMap.of(
          "catalog", "cat",
          "database", "db",
          "table", "tbl",
          "group", OPTIMIZER_GROUP);

  private MetricRegistry registry;

  @BeforeEach
  public void setUp() {
    registry = new MetricRegistry();
  }

  @AfterEach
  public void tearDown() {
    MetricFilterConfig.reset();
  }

  // --------------------------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------------------------

  /**
   * Install a {@link MetricFilterConfig} under LEGACY profile (which whitelists every status field
   * — the strictest case for the switch under test) with the given {@code skipZeroStatus} value.
   */
  private void installFilter(boolean skipZeroStatus) {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, skipZeroStatus);
    MetricFilterConfig.init(conf);
  }

  private void installFilterWithProfile(String profile, boolean skipZeroStatus) {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, profile);
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, skipZeroStatus);
    MetricFilterConfig.init(conf);
  }

  private boolean isRegistered(MetricDefine define) {
    return registry.getMetrics().containsKey(new MetricKey(define, TAGS));
  }

  /** Drive the table into the given status with {@code statusSetTimestamp = now}. */
  private void enter(TableOptimizingMetrics m, OptimizingStatus status) {
    m.statusChanged(status, System.currentTimeMillis());
  }

  /**
   * Drive the table into the given status with a {@code statusSetTimestamp} far in the past, so
   * that any subsequent {@link TableOptimizingMetrics#refresh()} call observes a strictly positive
   * duration that exceeds millisecond-level thresholds without {@code Thread.sleep} flakiness.
   */
  private void enterWithDuration(TableOptimizingMetrics m, OptimizingStatus status, long ageMs) {
    m.statusChanged(status, System.currentTimeMillis() - ageMs);
  }

  // --------------------------------------------------------------------------------------------
  // t5: switch off — keep upstream eager-registration behaviour
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOff_AllTenStatusMetricsRegisteredEagerly() {
    installFilter(false);
    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);

    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PLANNING));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_EXECUTING));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_COMMITTING));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IDLE_DURATION));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_PLANNING_DURATION));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_EXECUTING_DURATION));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_COMMITTING_DURATION));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // t6: switch on, IDLE bootstrap state — only status_in_idle registered (its value == 1).
  //     idle_duration starts ~0 ms and threshold is 0, so it is *not* registered initially
  //     (strict >). The remaining 8 fields are all 0 and thus unregistered.
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_BootstrapState_OnlyInIdleRegistered() {
    installFilter(true);
    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);
    // The bootstrap state of TableOptimizingMetrics is IDLE with statusSetTimestamp = ctor time;
    // we explicitly trigger a sync via statusChanged(IDLE, <now>) so that the dynamic path runs
    // even when DefaultOptimizingState is not involved.
    enter(m, OptimizingStatus.IDLE);

    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PLANNING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_EXECUTING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_COMMITTING));
    // The 4 duration metrics for non-IDLE states must be unregistered: their gauges return 0
    // outside their own state, so value (0) > threshold (0) is false.
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_PLANNING_DURATION));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_EXECUTING_DURATION));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_COMMITTING_DURATION));
    // idle_duration (gauge of the current state) is intentionally not asserted here: its value
    // is now - statusSetTimestamp, which racey microsecond-level deltas may or may not push above
    // the default 0 threshold. The behaviour is correct in both branches; flakiness is avoided.

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // t7: statusChanged(IDLE → PENDING). status_in_idle: 1→0 unregister; status_in_pending: 0→1
  //     register. The 5 non-PENDING duration metrics are all 0 (their gauge returns 0 when not
  //     in that state) and stay unregistered. pending_duration depends on the threshold: with
  //     default 0 and a fresh statusSetTimestamp ~ now, the value is also 0 ⇒ unregistered.
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_TransitionIdleToPending() {
    installFilter(true);
    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);
    enter(m, OptimizingStatus.IDLE);
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));

    enter(m, OptimizingStatus.PENDING);

    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PLANNING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_EXECUTING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_COMMITTING));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // t8: threshold crossing via refresh(). PENDING with statusSetTimestamp far in the past →
  //     pending_duration_mills > threshold ⇒ register on refresh().
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_DurationThresholdCrossingRegistersOnRefresh() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, true);
    // 1 ms threshold makes the test deterministic (we artificially backdate statusSetTimestamp
    // by 1 second, which is orders of magnitude larger than any clock skew on CI).
    conf.setLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING, 1L);
    MetricFilterConfig.init(conf);

    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);
    enterWithDuration(m, OptimizingStatus.PENDING, 1_000L);

    // Immediately after statusChanged the gauge value already exceeds 1 ms, so the dynamic
    // sync path inside statusChanged() registers it. Calling refresh() is idempotent.
    m.refresh();

    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));
    // Other 4 status_*_duration_mills must remain unregistered because their gauges return 0
    // outside their state.
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IDLE_DURATION));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_PLANNING_DURATION));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_EXECUTING_DURATION));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_COMMITTING_DURATION));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // t9: threshold fall-back via statusChanged(). After leaving PENDING the gauge returns 0,
  //     so pending_duration_mills must be unregistered on the very next statusChanged().
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_LeavingStateUnregistersDuration() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, true);
    conf.setLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING, 1L);
    MetricFilterConfig.init(conf);

    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);
    enterWithDuration(m, OptimizingStatus.PENDING, 1_000L);
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));

    // Leave PENDING — duration gauge returns 0 ⇒ should unregister.
    enter(m, OptimizingStatus.MINOR_OPTIMIZING);

    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_EXECUTING));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // t10: unregister() must clean up dynamically registered status keys (and reset the cached
  //      filter) so that a subsequent register() can rebuild from scratch.
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_UnregisterCleansAllDynamicallyRegisteredMetrics() {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, true);
    conf.setLong(AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING, 1L);
    MetricFilterConfig.init(conf);

    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);
    enterWithDuration(m, OptimizingStatus.PENDING, 1_000L);
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));

    m.unregister();

    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));
    // All other status keys must also be gone (none were registered at this point, but
    // verifying the registry is empty for them confirms there are no leftover entries).
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PLANNING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_EXECUTING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_COMMITTING));
  }

  // --------------------------------------------------------------------------------------------
  // t11: RECOMMENDED profile + skipZeroStatus=true. Profile blacklists 3 of the 5 status_in_*
  //      fields (in_idle / in_planning / in_committing) and all 5 duration fields. Even when
  //      the value would otherwise trigger registration, the profile gate must veto.
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_RecommendedProfile_HonoursProfileBlacklist() {
    installFilterWithProfile("recommended", true);
    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);
    enterWithDuration(m, OptimizingStatus.IDLE, 1_000L);

    // status_in_idle is blacklisted by RECOMMENDED — must remain unregistered even though
    // its value is 1.
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));
    // idle_duration is blacklisted as well — must remain unregistered even though its value
    // is > 0.
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IDLE_DURATION));

    // Switch to PENDING — status_in_pending is whitelisted by RECOMMENDED, so it must be
    // dynamically registered now that its value is 1. pending_duration is blacklisted and
    // must remain unregistered regardless of the duration value.
    enterWithDuration(m, OptimizingStatus.PENDING, 1_000L);
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_PENDING_DURATION));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // t12: LEGACY profile + skipZeroStatus=true. All 10 fields enter the gate; the value test
  //      decides registration. (Already partially exercised by t6~t10; this case adds explicit
  //      coverage for status_in_idle being whitelisted under LEGACY.)
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_LegacyProfile_AllStatusFieldsEnterTheGate() {
    installFilter(true);
    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);

    // IDLE state — status_in_idle is whitelisted under LEGACY ⇒ dynamic register on value==1.
    enter(m, OptimizingStatus.IDLE);
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));

    // Subsequent state changes must continue to update all 5 status_in_* fields.
    enter(m, OptimizingStatus.COMMITTING);
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_IDLE));
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_COMMITTING));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // t13: optimizerGroupChanged() does unregister() then register(). The dynamic status keys
  //      bookkeeping must be cleared by unregister() so that the new group's tags take effect
  //      cleanly.
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_OptimizerGroupChangedClearsDynamicStatusKeys() {
    installFilter(true);
    TableOptimizingMetrics m = new TableOptimizingMetrics(TABLE, OPTIMIZER_GROUP);
    m.register(registry);
    enter(m, OptimizingStatus.PENDING);
    Assertions.assertTrue(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));

    // Switch group — the existing tag set ("group"="default") must be unregistered, the new
    // tag set ("group"="g2") becomes effective on re-register.
    m.optimizerGroupChanged("g2");

    // Old-tag entries are gone.
    Assertions.assertFalse(isRegistered(TABLE_OPTIMIZING_STATUS_IN_PENDING));
    // New-tag entry exists for status_in_pending because the table is still in PENDING.
    ImmutableMap<String, String> newTags =
        ImmutableMap.of("catalog", "cat", "database", "db", "table", "tbl", "group", "g2");
    Assertions.assertTrue(
        registry
            .getMetrics()
            .containsKey(new MetricKey(TABLE_OPTIMIZING_STATUS_IN_PENDING, newTags)));

    m.unregister();
  }
}
