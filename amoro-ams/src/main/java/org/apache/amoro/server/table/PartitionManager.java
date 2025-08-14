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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.optimizing.plan.PartitionEvaluator;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.persistence.AmoroPatitionable;
import org.apache.amoro.server.persistence.PartitionMeta;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.PartitionMetaMapper;
import org.apache.amoro.table.MixedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PartitionManager {
  private static final Logger log = LoggerFactory.getLogger(PartitionManager.class);
  private static final Random RANDOM = new Random();
  private static final BlockingQueue<AmoroPatitionable> AMORO_PATITIONABLES =
      new LinkedBlockingQueue<>();
  private static boolean initialized = false;
  private static boolean ENABLE_PERSIST = false;
  private static int BATCH_SIZE = 200;
  private static PartitionPersistent partitionPersistent = null;
  private static ExpiredTablePersistent expiredTablePersistent = null;

  public static synchronized void init(Configurations configuration) {
    if (!initialized) {
      ENABLE_PERSIST = configuration.getBoolean(AmoroManagementConf.PARTITION_PERSIST_ENABLED);
      BATCH_SIZE = configuration.getInteger(AmoroManagementConf.PARTITION_PERSIST_BATCH_SIZE);
      if (ENABLE_PERSIST) {
        log.info("Initializing PartitionManager");
        partitionPersistent = new PartitionPersistent();
        expiredTablePersistent = new ExpiredTablePersistent();
        startRefreshPartitions();
        startDeleteExpiredTables();
        log.info("Initialized PartitionManager");
      } else {
        log.info("PartitionManager is not enabled");
      }
      initialized = true;
    }
  }

  private static void startRefreshPartitions() {
    Thread thread =
        new Thread(
            () -> {
              while (true) {
                AmoroPatitionable amoroPatitionable;
                try {
                  amoroPatitionable = AMORO_PATITIONABLES.take();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                if (amoroPatitionable instanceof PartitionMeta) {
                  List<PartitionMeta> partitions = new ArrayList<>();
                  partitions.add((PartitionMeta) amoroPatitionable);
                  int queueSize = AMORO_PATITIONABLES.size() + 1;
                  if (queueSize > BATCH_SIZE) {
                    log.info("queue size: {}", queueSize);
                  }
                  while (partitions.size() < Math.min(BATCH_SIZE, queueSize)
                      && AMORO_PATITIONABLES.peek() instanceof PartitionMeta) {
                    partitions.add((PartitionMeta) AMORO_PATITIONABLES.poll());
                  }
                  upsertPartitions(partitions);
                } else if (amoroPatitionable instanceof ExpiredPartition) {
                  deleteExpiredPartitions((ExpiredPartition) amoroPatitionable);
                }
              }
            });
    String name = "partition-refresh-thread";
    thread.setName(name);
    thread.start();
    log.info("{} started", name);
  }

  private static void startDeleteExpiredTables() {
    Thread thread =
        new Thread(
            () -> {
              while (true) {
                List<ExpiredTable> expiredTables = null;
                try {
                  expiredTables = expiredTablePersistent.selectExpiredTables(BATCH_SIZE);
                } catch (Throwable e) {
                  log.warn("failed to select expired tables", e);
                }
                log.info("{} expired tables", expiredTables == null ? 0 : expiredTables.size());
                if (expiredTables != null && !expiredTables.isEmpty()) {
                  expiredTables.forEach(PartitionManager::deleteExpiredTable);
                } else {
                  try {
                    Thread.sleep(2 * 60 * 1000);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
              }
            });
    String name = "partition-clean-thread";
    thread.setName(name);
    thread.start();
    log.info("{} started", name);
  }

  private static void upsertPartitions(List<PartitionMeta> partitions) {
    long start = System.currentTimeMillis();
    log.info("starting to upsert {} partitions", partitions.size());
    Long res =
        executeWithRetry(
            30, RANDOM.nextInt(30) + 2, () -> partitionPersistent.upsertPartitions(partitions));
    if (res != null) {
      long updated = res - partitions.size();
      long inserted = partitions.size() - updated;
      log.info(
          "upserted {} partitions within {}ms, inserted:{}, updated:{}",
          partitions.size(),
          System.currentTimeMillis() - start,
          inserted,
          updated);
    } else {
      log.warn(
          "failed to upsert {} partitions within {}ms",
          partitions.size(),
          System.currentTimeMillis() - start);
    }
  }

  private static void deleteExpiredPartitions(ExpiredPartition expiredPartition) {
    long start = System.currentTimeMillis();
    log.info("starting to delete partitions: {}", expiredPartition);
    Long deleted;
    long totalDeleted = 0;
    do {
      long currentTime = System.currentTimeMillis();
      deleted =
          executeWithRetry(
              10,
              RANDOM.nextInt(5) + 1,
              () ->
                  partitionPersistent.deletePartitionsExceptSnapshot(
                      expiredPartition.tableId, expiredPartition.validSnapshot, BATCH_SIZE));
      if (deleted != null) {
        totalDeleted += deleted;
        log.info(
            "deleted {} partitions of {} within {}ms",
            deleted,
            expiredPartition,
            System.currentTimeMillis() - currentTime);
      } else {
        log.warn(
            "failed to delete partitions of {} within {}ms",
            expiredPartition,
            System.currentTimeMillis() - currentTime);
      }
    } while (deleted == null || deleted == BATCH_SIZE);
    log.info(
        "deleted {} partitions of {} in total within {}ms",
        totalDeleted,
        expiredPartition,
        System.currentTimeMillis() - start);
  }

  public static void refreshPartitions(
      AmoroTable<?> amoroTable, TableRuntime tableRuntime, AbstractOptimizingEvaluator evaluator) {
    if (!ENABLE_PERSIST) {
      return;
    }
    MixedTable mixedTable = (MixedTable) amoroTable.originalTable();
    if (evaluator == null
        || mixedTable.spec() == null
        || !mixedTable.spec().isPartitioned()) { // only for partitioned table
      return;
    }
    if (!evaluator.isInitialized()) {
      throw new RuntimeException(
          "can not persist partition since this evaluator is not yet initialized");
    }
    if (!evaluator.getPartitionPlanMap().isEmpty()) {
      List<PartitionMeta> partitionMetas =
          evaluator.getPartitionPlanMap().entrySet().stream()
              .map(
                  entry -> {
                    String partName = entry.getKey();
                    PartitionEvaluator partEvaluator = entry.getValue();

                    long deleteDataBytes =
                        partEvaluator.getPosDeleteFileSize()
                            + partEvaluator.getEqualityDeleteFileSize();
                    long deleteRecords =
                        partEvaluator.getPosDeleteFileRecords()
                            + partEvaluator.getEqualityDeleteFileRecords();
                    long deleteFiles =
                        partEvaluator.getPosDeleteFileCount()
                            + partEvaluator.getEqualityDeleteFileCount();

                    long totalFiles =
                        partEvaluator.getFragmentFileCount()
                            + partEvaluator.getSegmentFileCount()
                            + deleteFiles;
                    long totalDataBytes =
                        partEvaluator.getFragmentFileSize()
                            + partEvaluator.getSegmentFileSize()
                            + deleteDataBytes;
                    long totalRecords =
                        partEvaluator.getFragmentFileRecords()
                            + partEvaluator.getSegmentFileRecords()
                            + deleteRecords;

                    PartitionMeta partitionMeta =
                        new PartitionMeta(
                            tableRuntime.getTableIdentifier().getCatalog(),
                            tableRuntime.getTableIdentifier().getDatabase(),
                            tableRuntime.getTableIdentifier().getTableName(),
                            partName,
                            mixedTable.location() + "/data/" + partName,
                            amoroTable.currentSnapshot().id());
                    partitionMeta.setTableId(tableRuntime.getTableIdentifier().getId());
                    partitionMeta.setTotalFiles(totalFiles);
                    partitionMeta.setTotalDataBytes(totalDataBytes);
                    partitionMeta.setTotalRecords(totalRecords);
                    partitionMeta.setDeleteDataBytes(deleteDataBytes);
                    partitionMeta.setDeleteFiles(deleteFiles);
                    partitionMeta.setDeleteRecords(deleteRecords);
                    partitionMeta.setModifyTime(amoroTable.currentSnapshot().commitTime());

                    return partitionMeta;
                  })
              .collect(Collectors.toList());
      log.info(
          "will persist {} partitions of {}.{}.{} with snapshot id: {}",
          partitionMetas.size(),
          tableRuntime.getTableIdentifier().getCatalog(),
          tableRuntime.getTableIdentifier().getDatabase(),
          tableRuntime.getTableIdentifier().getTableName(),
          amoroTable.currentSnapshot().id());
      AMORO_PATITIONABLES.addAll(partitionMetas);
    }
    // must upsert part before deleting part
    deleteExpiredPartitions(tableRuntime, amoroTable.currentSnapshot().id());
  }

  private static void deleteExpiredTable(ExpiredTable expiredTable) {
    long start = System.currentTimeMillis();
    log.info("starting to clean partitions of {}", expiredTable);
    Long deleted;
    do {
      long currentTime = System.currentTimeMillis();
      deleted =
          executeWithRetry(
              20,
              RANDOM.nextInt(10) + 3,
              () ->
                  partitionPersistent.deletePartitionsByTableId(
                      expiredTable.getTableId(), BATCH_SIZE));
      if (deleted != null) {
        log.info(
            "cleaned {} partitions of {} within {}ms",
            deleted,
            expiredTable,
            System.currentTimeMillis() - currentTime);
      } else {
        log.warn(
            "failed to clean partitions of {} within {}ms",
            expiredTable,
            System.currentTimeMillis() - currentTime);
      }
    } while (deleted == null || deleted == BATCH_SIZE);
    // only delete the table without any partitions
    executeWithRetry(
        2,
        RANDOM.nextInt(5) + 1,
        () -> {
          expiredTablePersistent.deleteExpiredTableWithoutPartition(expiredTable.getTableId());
          return 1;
        });
    log.info(
        "finished cleaning partitions of {} within {}ms",
        expiredTable,
        System.currentTimeMillis() - start);
  }

  public static void deleteExpiredPartitions(TableRuntime tableRuntime, String validSnapshot) {
    if (ENABLE_PERSIST) {
      AMORO_PATITIONABLES.add(
          new ExpiredPartition(
              tableRuntime.getTableIdentifier().getCatalog(),
              tableRuntime.getTableIdentifier().getDatabase(),
              tableRuntime.getTableIdentifier().getTableName(),
              validSnapshot,
              tableRuntime.getTableIdentifier().getId()));
    }
  }

  public static void deleteExpiredPartitionByTable(TableRuntime tableRuntime) {
    if (ENABLE_PERSIST) {
      ExpiredTable expiredTable =
          new ExpiredTable(
              tableRuntime.getTableIdentifier().getId(),
              tableRuntime.getTableIdentifier().getCatalog(),
              tableRuntime.getTableIdentifier().getDatabase(),
              tableRuntime.getTableIdentifier().getTableName(),
              0);
      expiredTablePersistent.upsertExpiredTable(expiredTable);
    }
  }

  /** Handle the AMORO_PATITIONABLES queue */
  public static void dispose() {
    if (initialized) {
      log.info(
          "Disposing {} with queue size = {}",
          PartitionManager.class.getSimpleName(),
          AMORO_PATITIONABLES.size());
      long start = System.currentTimeMillis();
      long maxWait = 10 * 1000; // 10s
      while (!AMORO_PATITIONABLES.isEmpty() && System.currentTimeMillis() - start <= maxWait) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          break;
        }
      }
      log.info(
          "Disposed {} within {}ms",
          PartitionManager.class.getSimpleName(),
          System.currentTimeMillis() - start);
    }
  }

  private static class ExpiredPartition implements AmoroPatitionable {
    private final String catalog;
    private final String schema;
    private final String table;
    private final String validSnapshot;
    private final Long tableId;

    public ExpiredPartition(
        String catalog, String schema, String table, String validSnapshot, Long tableId) {
      this.catalog = catalog;
      this.schema = schema;
      this.table = table;
      this.validSnapshot = validSnapshot;
      this.tableId = tableId;
    }

    @Override
    public String toString() {
      return String.format(
          "ExpiredPartition: validSnapshot=%s#tableId=%s#name=%s.%s.%s",
          validSnapshot, tableId, catalog, schema, table);
    }
  }

  private static class PartitionPersistent extends PersistentBase {

    private static final Logger log = LoggerFactory.getLogger(PartitionPersistent.class);

    public Long upsertPartitions(List<PartitionMeta> partitions) {
      if (partitions != null && !partitions.isEmpty()) {
        return updateAs(PartitionMetaMapper.class, mapper -> mapper.upsertPartitions(partitions));
      }
      return 0L;
    }

    public Long deletePartitionsByTableId(long tableId, long batchSize) {
      return updateAs(
          PartitionMetaMapper.class,
          mapper -> mapper.deletePartitionsByTableId(tableId, batchSize));
    }

    public Long deletePartitionsExceptSnapshot(long tableId, String snapshotId, long batchSize) {
      return updateAs(
          PartitionMetaMapper.class,
          mapper -> mapper.deletePartitionsExceptSnapshot(tableId, snapshotId, batchSize));
    }

    public List<ServerTableIdentifier> getNonExistTables(long batchSize) {
      return getAs(PartitionMetaMapper.class, mapper -> mapper.selectNoneExistTables(batchSize));
    }

    public List<Long> getTableWithMultiSnapshots(long batchSize) {
      return getAs(
          PartitionMetaMapper.class, mapper -> mapper.selectTableWithMultiSnapshots(batchSize));
    }
  }

  private static <T> T executeWithRetry(int maxAttempts, long delaySeconds, Supplier<T> task) {
    for (int i = 0; i < maxAttempts; i++) {
      try {
        return task.get();
      } catch (Throwable t) {
        if (++i == maxAttempts) {
          log.error("fail to persistent partition, reason: ", t);
          break;
        } else {
          log.warn(
              "fail to persistent partition, reason:{}, will retry after {}s",
              t.getMessage(),
              delaySeconds);
          try {
            Thread.sleep(delaySeconds * 1000);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    }
    return null;
  }
}
