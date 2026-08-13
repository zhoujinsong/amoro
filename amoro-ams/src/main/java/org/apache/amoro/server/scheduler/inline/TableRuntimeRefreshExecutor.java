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

import java.lang.reflect.Method;

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
    AmoroTable<?> table = null;
    boolean isLanceTable = false;
    try {
      Preconditions.checkArgument(tableRuntime instanceof DefaultTableRuntime);
      DefaultTableRuntime defaultTableRuntime = (DefaultTableRuntime) tableRuntime;

      DefaultOptimizingState optimizingState = defaultTableRuntime.getOptimizingState();
      table = loadTable(tableRuntime);

      // Lance tables go through a separate refresh path and do not participate in optimizing
      // evaluation
      if (TableFormat.LANCE.equals(table.format())) {
        isLanceTable = true;
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
    } finally {
      // Lance Dataset (org.lance.Dataset implements Closeable) holds ArrowBuf and native memory
      // that are not reclaimed by GC. Dataset is opened without an explicit allocator, so
      // selfManagedAllocator=true and Dataset.close() releases all off-heap memory. Each
      // loadTable() creates a new Dataset, so it must be closed after use to avoid memory leak.
      if (isLanceTable && table != null) {
        closeLanceDataset(table);
      }
    }
  }

  /**
   * Refresh logic for Lance tables: refresh snapshots + build tableSummary by reading the
   * ManifestSummary via reflection.
   */
  private void executeLanceRefresh(
      DefaultTableRuntime tableRuntime,
      DefaultOptimizingState optimizingState,
      AmoroTable<?> table) {
    try {
      optimizingState.refresh(table);
      // Read Lance Dataset statistics via reflection to avoid a direct dependency on the lance
      // module
      Object dataset = table.originalTable();
      // Empty Lance table (registered in HMS but dataset not yet created):
      // skip reflection and write a zero-valued summary
      if (dataset == null) {
        AbstractOptimizingEvaluator.PendingInput emptySummary =
            AbstractOptimizingEvaluator.PendingInput.ofLance(0, 0L, 0L, 0, 0L, 0L);
        optimizingState.setTableSummary(emptySummary);
        logger.debug(
            "Refreshed Lance table {} summary with empty placeholder (originalTable is null)",
            tableRuntime.getTableIdentifier());
        return;
      }
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
      logger.debug(
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

  /**
   * Close the Lance Dataset to release ArrowBuf and native memory. Called via reflection to avoid a
   * compile-time dependency on the lance module.
   *
   * <p>Lance Dataset is opened without an explicit allocator (see LanceRestCatalog.loadTable /
   * LanceHiveCatalog.loadTable), so the SDK sets selfManagedAllocator=true internally. This means
   * Dataset.close() will release all off-heap memory (ArrowBuf, native pointers) owned by the
   * Dataset. Without this close call, every loadTable() leaks a full Dataset object including its
   * Arrow Schema (JsonArrowField list) and ArrowBuf instances.
   */
  private void closeLanceDataset(AmoroTable<?> table) {
    Object dataset = table.originalTable();
    if (dataset == null) {
      return;
    }
    try {
      Method closeMethod = dataset.getClass().getMethod("close");
      closeMethod.invoke(dataset);
    } catch (Exception e) {
      logger.warn("Failed to close Lance dataset for {}", table.id(), e);
    }
  }
}
