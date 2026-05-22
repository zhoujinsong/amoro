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

package org.apache.amoro.server.scheduler.inline;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.scheduler.PeriodicTableScheduler;
import org.apache.amoro.server.table.DefaultOptimizingState;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.PartitionManager;
import org.apache.amoro.server.table.TableService;
import org.apache.amoro.server.utils.IcebergTableUtil;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.table.MixedTable;

/** Executor that refreshes table runtimes and evaluates optimizing status periodically. */
public class TableRuntimeRefreshExecutor extends PeriodicTableScheduler {

  // 1 minutes
  private final long interval;
  private final int maxPendingPartitions;

  public TableRuntimeRefreshExecutor(
      TableService tableService, int poolSize, long interval, int maxPendingPartitions) {
    super(tableService, poolSize);
    this.interval = interval;
    this.maxPendingPartitions = maxPendingPartitions;
  }

  @Override
  protected boolean formatSupported(TableFormat format) {
    return super.formatSupported(format) || TableFormat.LANCE.equals(format);
  }

  @Override
  protected boolean enabled(TableRuntime tableRuntime) {
    return tableRuntime instanceof DefaultTableRuntime;
  }

  @Override
  protected long getNextExecutingTime(TableRuntime tableRuntime) {
    DefaultTableRuntime defaultTableRuntime = (DefaultTableRuntime) tableRuntime;
    return Math.min(
        defaultTableRuntime.getOptimizingState().getOptimizingConfig().getMinorLeastInterval()
            * 4L
            / 5,
        interval);
  }

  public AbstractOptimizingEvaluator tryEvaluatingPendingInput(
      DefaultTableRuntime tableRuntime, MixedTable table) {
    DefaultOptimizingState optimizingState = tableRuntime.getOptimizingState();
    if (optimizingState.isOptimizingEnabled()
        && !optimizingState.getOptimizingStatus().isProcessing()) {
      AbstractOptimizingEvaluator evaluator =
          IcebergTableUtil.createOptimizingEvaluator(tableRuntime, table, maxPendingPartitions);
      if (evaluator.isNecessary()) {
        AbstractOptimizingEvaluator.PendingInput pendingInput =
            evaluator.getOptimizingPendingInput();
        logger.debug(
            "{} optimizing is necessary and get pending input {}",
            tableRuntime.getTableIdentifier(),
            pendingInput);
        optimizingState.setPendingInput(pendingInput);
      } else {
        optimizingState.optimizingNotNecessary();
      }
      optimizingState.setTableSummary(evaluator.getPendingInput());
      return evaluator;
    }
    return null;
  }

  @Override
  public void handleConfigChanged(TableRuntime tableRuntime, TableConfiguration originalConfig) {
    Preconditions.checkArgument(tableRuntime instanceof DefaultTableRuntime);
    DefaultTableRuntime defaultTableRuntime = (DefaultTableRuntime) tableRuntime;
    // After disabling self-optimizing, close the currently running optimizing process.
    if (originalConfig.getOptimizingConfig().isEnabled()
        && !tableRuntime.getTableConfiguration().getOptimizingConfig().isEnabled()) {
      OptimizingProcess optimizingProcess =
          defaultTableRuntime.getOptimizingState().getOptimizingProcess();
      if (optimizingProcess != null && optimizingProcess.getStatus() == ProcessStatus.RUNNING) {
        optimizingProcess.close();
      }
    }
  }

  @Override
  protected long getExecutorDelay() {
    return 0;
  }

  @Override
  public void execute(TableRuntime tableRuntime) {
    try {
      Preconditions.checkArgument(tableRuntime instanceof DefaultTableRuntime);
      DefaultTableRuntime defaultTableRuntime = (DefaultTableRuntime) tableRuntime;

      DefaultOptimizingState optimizingState = defaultTableRuntime.getOptimizingState();
      AmoroTable<?> table = loadTable(tableRuntime);

      // Lance 表走独立的刷新逻辑，不参与优化评估
      if (TableFormat.LANCE.equals(table.format())) {
        executeLanceRefresh(defaultTableRuntime, optimizingState, table);
        return;
      }

      long lastOptimizedSnapshotId = optimizingState.getLastOptimizedSnapshotId();
      long lastOptimizedChangeSnapshotId = optimizingState.getLastOptimizedChangeSnapshotId();
      optimizingState.refresh(table);
      MixedTable mixedTable = (MixedTable) table.originalTable();
      if ((mixedTable.isKeyedTable()
              && (lastOptimizedSnapshotId != optimizingState.getCurrentSnapshotId()
                  || lastOptimizedChangeSnapshotId != optimizingState.getCurrentChangeSnapshotId()))
          || (mixedTable.isUnkeyedTable()
              && lastOptimizedSnapshotId != optimizingState.getCurrentSnapshotId())) {
        AbstractOptimizingEvaluator evaluator =
            tryEvaluatingPendingInput(defaultTableRuntime, mixedTable);
        PartitionManager.refreshPartitions(table, tableRuntime, evaluator);
      }
    } catch (Throwable throwable) {
      logger.error("Refreshing table {} failed.", tableRuntime.getTableIdentifier(), throwable);
    }
  }

  /** Lance 表的刷新逻辑：刷新快照 + 通过反射获取 ManifestSummary 构建 tableSummary。 */
  private void executeLanceRefresh(
      DefaultTableRuntime tableRuntime,
      DefaultOptimizingState optimizingState,
      AmoroTable<?> table) {
    try {
      optimizingState.refresh(table);
      // 通过反射获取 Lance Dataset 的统计信息，避免直接依赖 lance 模块
      Object dataset = table.originalTable();
      Object version = dataset.getClass().getMethod("getVersion").invoke(dataset);
      Object summary = version.getClass().getMethod("getManifestSummary").invoke(version);
      long totalFilesSize =
          (long) summary.getClass().getMethod("getTotalFilesSize").invoke(summary);
      long totalDataFiles =
          (long) summary.getClass().getMethod("getTotalDataFiles").invoke(summary);
      long totalRows = (long) summary.getClass().getMethod("getTotalRows").invoke(summary);
      long totalDeletionFiles =
          (long) summary.getClass().getMethod("getTotalDeletionFiles").invoke(summary);
      long totalDataFileRows =
          (long) summary.getClass().getMethod("getTotalDataFileRows").invoke(summary);
      long totalDeletionFileRows =
          (long) summary.getClass().getMethod("getTotalDeletionFileRows").invoke(summary);

      AbstractOptimizingEvaluator.PendingInput tableSummary =
          AbstractOptimizingEvaluator.PendingInput.ofLance(
              (int) totalDataFiles,
              totalFilesSize,
              totalRows,
              (int) totalDeletionFiles,
              totalDataFileRows,
              totalDeletionFileRows);
      optimizingState.setTableSummary(tableSummary);
      logger.info(
          "Refreshed Lance table {} summary: dataFiles={}, deletionFiles={}, fileSize={},"
              + " dataRows={}, deletionRows={}, totalRows={}",
          tableRuntime.getTableIdentifier(),
          totalDataFiles,
          totalDeletionFiles,
          totalFilesSize,
          totalDataFileRows,
          totalDeletionFileRows,
          totalRows);
    } catch (Throwable throwable) {
      logger.error(
          "Refreshing Lance table {} failed.", tableRuntime.getTableIdentifier(), throwable);
    }
  }
}
