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

import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_DANGLING_DELETE_FILES;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_EQUALITY_DELETE_FILES;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_HEALTH_SCORE;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_POSITION_DELETE_FILES;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.metrics.MetricFilterConfig;
import org.apache.amoro.server.metrics.MetricRegistry;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code metric.summary.skip-zero-deletes} value-based filter implemented in
 * {@link TableSummaryMetrics}.
 *
 * <p>Each test installs a {@link MetricFilterConfig} (LEGACY profile keeps every field allowed,
 * which is the strictest case for the switch) with the desired {@code skipZeroDeletes} value and
 * then asserts the registered set on a fresh {@link MetricRegistry}. The seven covered fields are:
 * equality / position / dangling delete files, equality / position delete sizes, equality /
 * position delete records.
 */
public class TestTableSummaryMetricsSkipZeroDeletes {

  private static final ServerTableIdentifier TABLE =
      ServerTableIdentifier.of(1L, "cat", "db", "tbl", TableFormat.ICEBERG);

  private static final ImmutableMap<String, String> TAGS =
      ImmutableMap.of("catalog", "cat", "database", "db", "table", "tbl");

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

  private void installFilter(boolean skipZeroDeletes) {
    Configurations conf = new Configurations();
    conf.setString(AmoroManagementConf.METRIC_PROFILE, "legacy");
    conf.setBoolean(AmoroManagementConf.METRIC_SUMMARY_SKIP_ZERO_DELETES, skipZeroDeletes);
    MetricFilterConfig.init(conf);
  }

  private boolean isRegistered(MetricDefine define) {
    return registry.getMetrics().containsKey(new MetricKey(define, TAGS));
  }

  /**
   * Build a {@link AbstractOptimizingEvaluator.PendingInput} mock with the provided values for the
   * seven delete-related fields. Non-delete fields default to 0; {@code healthScore} returns -1
   * (matches the production default and is unused by the switch under test).
   */
  private AbstractOptimizingEvaluator.PendingInput pending(
      int equalityFiles,
      int positionFiles,
      int danglingFiles,
      long equalityBytes,
      long positionBytes,
      long equalityRecords,
      long positionRecords) {
    AbstractOptimizingEvaluator.PendingInput p =
        mock(AbstractOptimizingEvaluator.PendingInput.class);
    when(p.getEqualityDeleteFileCount()).thenReturn(equalityFiles);
    when(p.getPositionalDeleteFileCount()).thenReturn(positionFiles);
    when(p.getDanglingDeleteFileCount()).thenReturn(danglingFiles);
    when(p.getEqualityDeleteBytes()).thenReturn(equalityBytes);
    when(p.getPositionalDeleteBytes()).thenReturn(positionBytes);
    when(p.getEqualityDeleteFileRecords()).thenReturn(equalityRecords);
    when(p.getPositionalDeleteFileRecords()).thenReturn(positionRecords);
    when(p.getHealthScore()).thenReturn(-1);
    when(p.getTotalFileCount()).thenReturn(0);
    when(p.getTotalFileSize()).thenReturn(0L);
    when(p.getTotalFileRecords()).thenReturn(0L);
    when(p.getDataFileCount()).thenReturn(0);
    when(p.getDataFileSize()).thenReturn(0L);
    when(p.getDataFileRecords()).thenReturn(0L);
    return p;
  }

  private AbstractOptimizingEvaluator.PendingInput allZero() {
    return pending(0, 0, 0, 0L, 0L, 0L, 0L);
  }

  // --------------------------------------------------------------------------------------------
  // Switch off — keep upstream eager-registration behaviour
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOff_AllSevenDeleteMetricsRegisteredEagerly() {
    installFilter(false);
    TableSummaryMetrics m = new TableSummaryMetrics(TABLE);
    m.register(registry);

    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_DANGLING_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS));
    // Health score is always allowed under LEGACY and never gated by the switch.
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_HEALTH_SCORE));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // Switch on — initial state with all-zero delete fields
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_InitialAllZero_NoneOfSevenRegistered() {
    installFilter(true);
    TableSummaryMetrics m = new TableSummaryMetrics(TABLE);
    m.register(registry);

    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_DANGLING_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS));
    // Health score is unaffected by the switch — must remain registered.
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_HEALTH_SCORE));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // Switch on — independent per-metric judgement
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_PartialNonZero_OnlyMatchingMetricsRegistered() {
    installFilter(true);
    TableSummaryMetrics m = new TableSummaryMetrics(TABLE);
    m.register(registry);

    // Only equality file count and position records are non-zero.
    m.refresh(pending(5, 0, 0, 0L, 0L, 0L, 99L));

    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS));
    // The remaining five must stay unregistered: each metric is judged independently
    // against its own value (no joint condition).
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_DANGLING_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // Switch on — state machine: 0 → non-zero → 0
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_DynamicRegisterOnZeroToNonZero() {
    installFilter(true);
    TableSummaryMetrics m = new TableSummaryMetrics(TABLE);
    m.register(registry);

    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));

    m.refresh(pending(3, 0, 0, 0L, 0L, 0L, 0L));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));

    m.unregister();
  }

  @Test
  public void testSwitchOn_DynamicUnregisterOnNonZeroToZero() {
    installFilter(true);
    TableSummaryMetrics m = new TableSummaryMetrics(TABLE);
    m.register(registry);

    m.refresh(pending(3, 0, 0, 0L, 0L, 0L, 0L));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));

    m.refresh(allZero());
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // Switch on — repeated refreshes must remain idempotent
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_RepeatedRefreshIsIdempotent() {
    installFilter(true);
    TableSummaryMetrics m = new TableSummaryMetrics(TABLE);
    m.register(registry);

    AbstractOptimizingEvaluator.PendingInput input = pending(7, 11, 0, 0L, 0L, 0L, 0L);
    m.refresh(input);
    m.refresh(input);
    m.refresh(input);

    // No duplicate registration / no exception thrown by the registry.
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_DANGLING_DELETE_FILES));

    m.unregister();
  }

  // --------------------------------------------------------------------------------------------
  // Switch on — table unregister must clean up dynamically registered metrics
  // --------------------------------------------------------------------------------------------

  @Test
  public void testSwitchOn_UnregisterCleansAllDynamicallyRegisteredMetrics() {
    installFilter(true);
    TableSummaryMetrics m = new TableSummaryMetrics(TABLE);
    m.register(registry);

    m.refresh(pending(5, 6, 7, 8L, 9L, 10L, 11L));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_DANGLING_DELETE_FILES));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS));
    Assertions.assertTrue(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS));

    m.unregister();

    // Every dynamically-registered delete metric must be gone, plus the always-allowed
    // health_score that was registered eagerly.
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_DANGLING_DELETE_FILES));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS));
    Assertions.assertFalse(isRegistered(TABLE_SUMMARY_HEALTH_SCORE));
  }
}
