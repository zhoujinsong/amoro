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

import static org.apache.amoro.metrics.MetricDefine.defineGauge;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_DANGLING_DELETE_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_DATA_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_DATA_FILES_RECORDS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_DATA_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_EQUALITY_DELETE_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_EQUALITY_DELETE_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_POSITION_DELETE_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_POSITION_DELETE_FILES_RECORDS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_POSITION_DELETE_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_SNAPSHOTS;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_TOTAL_FILES;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_TOTAL_FILES_SIZE;
import static org.apache.amoro.server.metrics.MetricFilterConfig.F_SUMMARY_TOTAL_RECORDS;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.server.metrics.MetricFilterConfig;
import org.apache.amoro.server.metrics.MetricRegistry;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.UnkeyedTable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/** Table Summary metrics. */
public class TableSummaryMetrics extends AbstractTableMetrics {

  // table summary files number metrics
  public static final MetricDefine TABLE_SUMMARY_TOTAL_FILES =
      defineGauge("table_summary_total_files")
          .withDescription("Total number of files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DATA_FILES =
      defineGauge("table_summary_data_files")
          .withDescription("Number of data files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_EQUALITY_DELETE_FILES =
      defineGauge("table_summary_equality_delete_files")
          .withDescription("Number of equality delete files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_POSITION_DELETE_FILES =
      defineGauge("table_summary_position_delete_files")
          .withDescription("Number of position delete files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DANGLING_DELETE_FILES =
      defineGauge("table_summary_dangling_delete_files")
          .withDescription("Number of dangling delete files in the table")
          .withTags("catalog", "database", "table")
          .build();

  // table summary files size metrics
  public static final MetricDefine TABLE_SUMMARY_TOTAL_FILES_SIZE =
      defineGauge("table_summary_total_files_size")
          .withDescription("Total size of files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DATA_FILES_SIZE =
      defineGauge("table_summary_data_files_size")
          .withDescription("Size of data files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE =
      defineGauge("table_summary_equality_delete_files_size")
          .withDescription("Size of equality delete files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE =
      defineGauge("table_summary_position_delete_files_size")
          .withDescription("Size of position delete files in the table")
          .withTags("catalog", "database", "table")
          .build();

  // table summary files records metrics
  public static final MetricDefine TABLE_SUMMARY_TOTAL_RECORDS =
      defineGauge("table_summary_total_records")
          .withDescription("Total records in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DATA_FILES_RECORDS =
      defineGauge("table_summary_data_files_records")
          .withDescription("Records of data files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS =
      defineGauge("table_summary_equality_delete_files_records")
          .withDescription("Records of equality delete files in the table")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS =
      defineGauge("table_summary_position_delete_files_records")
          .withDescription("Records of position delete files in the table")
          .withTags("catalog", "database", "table")
          .build();

  // table summary snapshots number metric
  public static final MetricDefine TABLE_SUMMARY_SNAPSHOTS =
      defineGauge("table_summary_snapshots")
          .withDescription("Number of snapshots in the table")
          .withTags("catalog", "database", "table")
          .build();

  // table summary health score metric
  public static final MetricDefine TABLE_SUMMARY_HEALTH_SCORE =
      defineGauge("table_summary_health_score")
          .withDescription("Health score of the table")
          .withTags("catalog", "database", "table")
          .build();

  /**
   * 受 {@code metric.summary.skip-zero-deletes} 全局开关管控的 7 个 delete-related field 与其 (define,
   * value-extractor, metric-factory) 的固定映射。每个条目独立判断、独立 register / unregister，互不联动。
   */
  private static final Map<String, DeleteMetricSpec> DELETE_METRIC_SPECS = buildDeleteSpecs();

  private static Map<String, DeleteMetricSpec> buildDeleteSpecs() {
    Map<String, DeleteMetricSpec> specs = new LinkedHashMap<>();
    specs.put(
        F_SUMMARY_EQUALITY_DELETE_FILES,
        new DeleteMetricSpec(
            TABLE_SUMMARY_EQUALITY_DELETE_FILES,
            m -> m.tableSummary.getEqualityDeleteFileCount(),
            m -> (Gauge<Long>) () -> (long) m.tableSummary.getEqualityDeleteFileCount()));
    specs.put(
        F_SUMMARY_POSITION_DELETE_FILES,
        new DeleteMetricSpec(
            TABLE_SUMMARY_POSITION_DELETE_FILES,
            m -> m.tableSummary.getPositionalDeleteFileCount(),
            m -> (Gauge<Long>) () -> (long) m.tableSummary.getPositionalDeleteFileCount()));
    specs.put(
        F_SUMMARY_DANGLING_DELETE_FILES,
        new DeleteMetricSpec(
            TABLE_SUMMARY_DANGLING_DELETE_FILES,
            m -> m.tableSummary.getDanglingDeleteFileCount(),
            m -> (Gauge<Long>) () -> (long) m.tableSummary.getDanglingDeleteFileCount()));
    specs.put(
        F_SUMMARY_EQUALITY_DELETE_FILES_SIZE,
        new DeleteMetricSpec(
            TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE,
            m -> m.tableSummary.getEqualityDeleteBytes(),
            m -> (Gauge<Long>) () -> m.tableSummary.getEqualityDeleteBytes()));
    specs.put(
        F_SUMMARY_POSITION_DELETE_FILES_SIZE,
        new DeleteMetricSpec(
            TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE,
            m -> m.tableSummary.getPositionalDeleteBytes(),
            m -> (Gauge<Long>) () -> m.tableSummary.getPositionalDeleteBytes()));
    specs.put(
        F_SUMMARY_EQUALITY_DELETE_FILES_RECORDS,
        new DeleteMetricSpec(
            TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS,
            m -> m.tableSummary.getEqualityDeleteFileRecords(),
            m -> (Gauge<Long>) () -> m.tableSummary.getEqualityDeleteFileRecords()));
    specs.put(
        F_SUMMARY_POSITION_DELETE_FILES_RECORDS,
        new DeleteMetricSpec(
            TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS,
            m -> m.tableSummary.getPositionalDeleteFileRecords(),
            m -> (Gauge<Long>) () -> m.tableSummary.getPositionalDeleteFileRecords()));
    return specs;
  }

  private AbstractOptimizingEvaluator.PendingInput tableSummary =
      new AbstractOptimizingEvaluator.PendingInput();

  private long snapshots = 0L;

  /** 当前 skip-zero-deletes 路径下已动态注册的 delete field → MetricKey 映射。 */
  private final Map<String, MetricKey> dynamicDeleteKeys = new HashMap<>();

  /** 注册时刻冻结的 filter，确保 register/refresh/unregister 三个阶段使用同一份配置。 */
  private MetricFilterConfig filter;

  public TableSummaryMetrics(ServerTableIdentifier identifier) {
    super(identifier);
  }

  @Override
  public void registerMetrics(MetricRegistry registry) {
    if (globalRegistry != null) {
      return;
    }
    this.filter = MetricFilterConfig.current();

    // register files number metrics
    register(
        filter,
        registry,
        F_SUMMARY_TOTAL_FILES,
        TABLE_SUMMARY_TOTAL_FILES,
        (Gauge<Long>) () -> (long) tableSummary.getTotalFileCount());
    register(
        filter,
        registry,
        F_SUMMARY_DATA_FILES,
        TABLE_SUMMARY_DATA_FILES,
        (Gauge<Long>) () -> (long) tableSummary.getDataFileCount());

    // register delete-related metrics — gated by both profile (allowSummary)
    // and the global skip-zero-deletes switch (per-metric value check).
    registerDeleteMetricsInitial(registry);

    // register files size metrics (non-delete)
    register(
        filter,
        registry,
        F_SUMMARY_TOTAL_FILES_SIZE,
        TABLE_SUMMARY_TOTAL_FILES_SIZE,
        (Gauge<Long>) () -> tableSummary.getTotalFileSize());
    register(
        filter,
        registry,
        F_SUMMARY_DATA_FILES_SIZE,
        TABLE_SUMMARY_DATA_FILES_SIZE,
        (Gauge<Long>) () -> tableSummary.getDataFileSize());

    // register files records metrics (non-delete)
    register(
        filter,
        registry,
        F_SUMMARY_TOTAL_RECORDS,
        TABLE_SUMMARY_TOTAL_RECORDS,
        (Gauge<Long>) () -> tableSummary.getTotalFileRecords());
    register(
        filter,
        registry,
        F_SUMMARY_DATA_FILES_RECORDS,
        TABLE_SUMMARY_DATA_FILES_RECORDS,
        (Gauge<Long>) () -> tableSummary.getDataFileRecords());

    // [内部分支硬屏蔽] amoro_table_summary_health_score 指标基数大 (~12万 series)
    // 且对运营价值低，为控制 /metrics 响应体积，直接不注册。
    // 如需恢复，删除本段注释即可。
    // register(
    //     filter,
    //     registry,
    //     F_SUMMARY_HEALTH_SCORE,
    //     TABLE_SUMMARY_HEALTH_SCORE,
    //     (Gauge<Long>) () -> (long) tableSummary.getHealthScore());

    // register snapshots number metric
    register(
        filter,
        registry,
        F_SUMMARY_SNAPSHOTS,
        TABLE_SUMMARY_SNAPSHOTS,
        (Gauge<Long>) () -> snapshots);

    globalRegistry = registry;
  }

  /**
   * 在 {@link #registerMetrics(MetricRegistry)} 阶段处理 7 个 delete metric：
   *
   * <ul>
   *   <li>当 {@code skipZeroDeletes == false}：保持社区行为，直接 eager 注册（前提是 profile 允许）。
   *   <li>当 {@code skipZeroDeletes == true}：完全跳过初始注册；后续每次 {@link
   *       #refresh(AbstractOptimizingEvaluator.PendingInput)} 时按各自当前值动态 register / unregister。
   * </ul>
   */
  private void registerDeleteMetricsInitial(MetricRegistry registry) {
    if (filter.isSkipZeroDeletes()) {
      // 动态路径：本阶段一律不注册，等 refresh 调用 syncDeleteMetricsByValue() 触发首次判定。
      return;
    }
    for (Map.Entry<String, DeleteMetricSpec> entry : DELETE_METRIC_SPECS.entrySet()) {
      String field = entry.getKey();
      DeleteMetricSpec spec = entry.getValue();
      register(filter, registry, field, spec.define, spec.metricFactory.apply(this));
    }
  }

  /** Register the metric only when the field is whitelisted by the current filter. */
  private void register(
      MetricFilterConfig filter,
      MetricRegistry registry,
      String field,
      MetricDefine define,
      Metric metric) {
    if (filter.allowSummary(field)) {
      registerMetric(registry, define, metric);
    }
  }

  public void refresh(AbstractOptimizingEvaluator.PendingInput tableSummary) {
    if (tableSummary == null) {
      return;
    }
    this.tableSummary = tableSummary;
    if (filter != null && filter.isSkipZeroDeletes() && globalRegistry != null) {
      syncDeleteMetricsByValue();
    }
  }

  /**
   * 对 7 个受管控的 delete metric 逐个独立执行"按值过滤"判定：
   *
   * <pre>
   *   value &gt; 0 且未注册 → register
   *   value == 0 且已注册 → unregister
   *   其他情况               → 维持现状
   * </pre>
   *
   * 7 个 metric 相互独立，不做任何联合判断（例如不会因为 file count 为 0 就联动反注册 size / records）。
   */
  private void syncDeleteMetricsByValue() {
    for (Map.Entry<String, DeleteMetricSpec> entry : DELETE_METRIC_SPECS.entrySet()) {
      String field = entry.getKey();
      if (!filter.allowSummary(field)) {
        // profile 已经禁用该字段，无论值多少都不应注册。
        continue;
      }
      DeleteMetricSpec spec = entry.getValue();
      long value = spec.valueExtractor.applyAsLong(this);
      boolean shouldRegister = value > 0L;
      boolean isRegistered = dynamicDeleteKeys.containsKey(field);

      if (shouldRegister && !isRegistered) {
        Metric metric = spec.metricFactory.apply(this);
        MetricKey key =
            globalRegistry.register(
                spec.define,
                ImmutableMap.of(
                    "catalog",
                    identifier.getCatalog(),
                    "database",
                    identifier.getDatabase(),
                    "table",
                    identifier.getTableName()),
                metric);
        registeredMetricKeys.add(key);
        dynamicDeleteKeys.put(field, key);
      } else if (!shouldRegister && isRegistered) {
        MetricKey key = dynamicDeleteKeys.remove(field);
        globalRegistry.unregister(key);
        registeredMetricKeys.remove(key);
      }
    }
  }

  @Override
  public void unregister() {
    super.unregister();
    dynamicDeleteKeys.clear();
    filter = null;
  }

  public void refreshSnapshots(MixedTable table) {
    UnkeyedTable unkeyedTable =
        table.isKeyedTable() ? table.asKeyedTable().baseTable() : table.asUnkeyedTable();
    snapshots = Lists.newArrayList(unkeyedTable.snapshots().iterator()).size();
  }

  /** Immutable spec entry for the seven managed delete-related metrics. */
  private static final class DeleteMetricSpec {
    final MetricDefine define;
    final ToLongFunction<TableSummaryMetrics> valueExtractor;
    final Function<TableSummaryMetrics, Metric> metricFactory;

    DeleteMetricSpec(
        MetricDefine define,
        ToLongFunction<TableSummaryMetrics> valueExtractor,
        Function<TableSummaryMetrics, Metric> metricFactory) {
      this.define = define;
      this.valueExtractor = valueExtractor;
      this.metricFactory = metricFactory;
    }
  }
}
