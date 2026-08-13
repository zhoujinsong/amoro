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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.metrics.MetricFilterConfig;
import org.apache.amoro.server.metrics.MetricRegistry;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Capacity-style end-to-end verification for the {@code metric.optimizing.skip-zero-status} filter.
 * Boots 100 in-memory {@link TableOptimizingMetrics} instances under a single shared {@link
 * MetricRegistry} with a realistic state distribution (90 IDLE / 5 PENDING / 5 EXECUTING) and
 * asserts that the registry size shrinks by ≥ 80% when the switch is turned on, matching the
 * acceptance criterion in {@code 2026-06-01-TableOptimizing指标精简方案.md} §4 T6.
 *
 * <p>This test does <em>not</em> spin up the AMS HTTP server — that would require half of the
 * project to be on the test classpath. Counting entries in {@link MetricRegistry#getMetrics()} is
 * authoritative because every {@code Reporter} (Prometheus included) sources its scrape payload
 * from the same map.
 */
public class TestSkipZeroStatusEndToEnd {

  private static final int TABLE_COUNT = 100;
  private static final int IDLE_COUNT = 90;
  private static final int PENDING_COUNT = 5;
  /** Remaining tables (TABLE_COUNT - IDLE_COUNT - PENDING_COUNT) are EXECUTING. */
  private static final String GROUP = "g";

  private MetricRegistry registry;
  private List<TableOptimizingMetrics> tables;

  @BeforeEach
  public void setUp() {
    registry = new MetricRegistry();
    tables = new ArrayList<>(TABLE_COUNT);
  }

  @AfterEach
  public void tearDown() {
    for (TableOptimizingMetrics m : tables) {
      m.unregister();
    }
    MetricFilterConfig.reset();
  }

  // --------------------------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------------------------

  /**
   * Bootstrap {@code TABLE_COUNT} tables under LEGACY profile with the supplied switch / threshold
   * combination, and drive each table into its assigned status with a {@code statusSetTimestamp}
   * that puts the duration well above any millisecond-level threshold (so PENDING/EXECUTING tables
   * would expose a duration metric whenever the threshold permits).
   */
  private void bootstrap(
      boolean skipZeroStatus, long idleThreshold, long pendingThreshold, long executingThreshold) {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_OPTIMIZING_SKIP_ZERO_STATUS, skipZeroStatus);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_IDLE, idleThreshold);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_PENDING, pendingThreshold);
    conf.setLong(
        AmoroManagementConf.METRIC_OPTIMIZING_STATUS_DURATION_THRESHOLD_EXECUTING,
        executingThreshold);
    MetricFilterConfig.init(conf);

    long backdate = 1_000L; // 1 second in the past

    for (int i = 0; i < TABLE_COUNT; i++) {
      ServerTableIdentifier id =
          ServerTableIdentifier.of((long) i, "cat", "db", "tbl_" + i, TableFormat.ICEBERG);
      TableOptimizingMetrics m = new TableOptimizingMetrics(id, GROUP);
      m.register(registry);

      OptimizingStatus status;
      if (i < IDLE_COUNT) {
        status = OptimizingStatus.IDLE;
      } else if (i < IDLE_COUNT + PENDING_COUNT) {
        status = OptimizingStatus.PENDING;
      } else {
        status = OptimizingStatus.MINOR_OPTIMIZING; // maps to STATUS_EXECUTING
      }
      m.statusChanged(status, System.currentTimeMillis() - backdate);
      tables.add(m);
    }
  }

  /**
   * Count only the status-class entries in the registry (10 metric names of interest), since we are
   * only testing the switch's effect on those. Non-status metrics (counters, since_last_*, lag)
   * inflate the total uniformly under both branches and would otherwise dilute the ratio.
   */
  private long countStatusSeries() {
    return registry.getMetrics().keySet().stream().filter(this::isStatusClass).count();
  }

  private boolean isStatusClass(MetricKey key) {
    String name = key.getDefine().getName();
    // 5 status_in_* + 5 status_*_duration_mills metrics — all share the
    // "table_optimizing_status_" prefix.
    return name.startsWith("table_optimizing_status_");
  }

  // --------------------------------------------------------------------------------------------
  // Capacity scenarios
  // --------------------------------------------------------------------------------------------

  /**
   * Baseline: switch off ⇒ every table eagerly registers all 10 status-class fields. Total = 100
   * tables × 10 = 1000 status-class series.
   */
  @Test
  public void testBaseline_SwitchOff_Has1000StatusSeries() {
    bootstrap(false, 0L, 0L, 0L);
    Assertions.assertEquals(TABLE_COUNT * 10L, countStatusSeries());
  }

  /**
   * Switch on, all thresholds = 0 (community-default semantics — any positive duration is exposed):
   *
   * <ul>
   *   <li>each IDLE table: 1 series for {@code status_in_idle} (=1) + 1 series for {@code
   *       status_idle_duration_mills} (1000 ms back-dated &gt; 0 ms threshold) = 2 series
   *   <li>each PENDING table: {@code status_in_pending} + {@code status_pending_duration_mills} = 2
   *       series
   *   <li>each EXECUTING table: {@code status_in_executing} + {@code
   *       status_executing_duration_mills} = 2 series
   * </ul>
   *
   * Total = 100 × 2 = 200 status-class series. The 4 duration metrics for non-current states are
   * never registered because their gauges return 0 outside their state.
   */
  @Test
  public void testSwitchOn_DefaultThresholds_Exactly200StatusSeries() {
    bootstrap(true, 0L, 0L, 0L);
    long count = countStatusSeries();
    Assertions.assertEquals(
        TABLE_COUNT * 2L,
        count,
        "Expected exactly " + (TABLE_COUNT * 2) + " status-class series, observed " + count);
    // Reduction from baseline 1000 ⇒ 80%.
    long baseline = TABLE_COUNT * 10L;
    Assertions.assertTrue(
        (baseline - count) * 100 / baseline >= 80,
        "Expected ≥ 80% reduction from " + baseline + ", observed " + count);
  }

  /**
   * Switch on with production-style thresholds (IDLE 1 min, PENDING 30 min, EXECUTING 1 hour). The
   * back-dated duration is only 1 s ⇒ no duration crosses its threshold ⇒ only the 100 {@code
   * status_in_*} series remain (one per table). 90% reduction from baseline.
   *
   * <p>This matches the recommended production configuration in {@code
   * docs/admin-guides/configuring-metrics.md} §7.4 (with a non-zero IDLE threshold added so the
   * test does not depend on community-default {@code idle=0} semantics).
   */
  @Test
  public void testSwitchOn_ProductionThresholds_Only100StatusInSeries() {
    bootstrap(true, 60_000L /* 1 min */, 1_800_000L /* 30 min */, 3_600_000L /* 1 hour */);
    long count = countStatusSeries();
    Assertions.assertEquals(
        TABLE_COUNT, count, "Expected exactly one status_in_* series per table, observed " + count);
  }

  /**
   * Repeatability: re-running the same scenario multiple times must yield deterministic counts — a
   * regression in {@code dynamicStatusKeys} bookkeeping (e.g. duplicate registration) would surface
   * here as registry-level {@code IllegalArgumentException}.
   */
  @Test
  public void testSwitchOn_RepeatedRefreshIsStable() {
    bootstrap(true, 60_000L, 1_800_000L, 3_600_000L);
    long firstCount = countStatusSeries();
    for (TableOptimizingMetrics m : tables) {
      m.refresh();
      m.refresh();
      m.refresh();
    }
    Assertions.assertEquals(firstCount, countStatusSeries());
  }
}
