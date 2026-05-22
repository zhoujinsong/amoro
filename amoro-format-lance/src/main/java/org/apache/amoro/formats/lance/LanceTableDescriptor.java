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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.descriptor.AMSColumnInfo;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.ConsumerInfo;
import org.apache.amoro.table.descriptor.DDLInfo;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.amoro.table.descriptor.PartitionBaseInfo;
import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TableSummary;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.lance.Branch;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.ManifestSummary;
import org.lance.Tag;
import org.lance.Version;
import org.lance.fragment.DataFile;
import org.lance.fragment.DeletionFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Table descriptor for Lance format. */
public class LanceTableDescriptor implements FormatTableDescriptor {

  private static final Logger LOG = LoggerFactory.getLogger(LanceTableDescriptor.class);

  public static final String TABLE_FORMAT = "table-format";
  private static final String OPERATION_TYPE_APPEND = "append";
  private static final String STORAGE_TYPE_TCLAKE = "TCLAKE";

  // Fragment 分区名前缀
  private static final String FRAGMENT_PREFIX = "fragment-";
  // 文件类型常量
  private static final String FILE_TYPE_DATA = "DATA_FILE";
  private static final String FILE_TYPE_DELETION_PREFIX = "DELETION_FILE_";
  // 删除文件路径格式
  private static final String DELETION_FILE_PATH_FORMAT = "_deletions/%d-%d-%d.arrow";
  // 优化类型常量
  private static final String OPTIMIZING_TYPE_COMPACTION_KEY = "COMPACTION";
  private static final String OPTIMIZING_TYPE_COMPACTION_VALUE = "compaction";
  // 分支名常量
  private static final String BRANCH_MAIN = "main";
  // baseMetric key 常量
  private static final String METRIC_VERSION = "version";
  private static final String METRIC_ROW_COUNT = "rowCount";
  private static final String METRIC_TOTAL_SIZE = "totalSize";
  private static final String METRIC_FILE_COUNT = "fileCount";
  private static final String METRIC_DATA_FILE_COUNT = "dataFileCount";
  private static final String METRIC_DELETION_FILE_COUNT = "deletionFileCount";
  private static final String METRIC_FRAGMENT_COUNT = "fragmentCount";
  private static final String METRIC_AVERAGE_FILE_SIZE = "averageFileSize";
  // versionSummary key 常量
  private static final String SUMMARY_VERSION = "version";
  private static final String SUMMARY_TOTAL_ROWS = "totalRows";
  private static final String SUMMARY_TOTAL_DATA_FILES = "totalDataFiles";
  private static final String SUMMARY_TOTAL_FILES_SIZE = "totalFilesSize";
  private static final String SUMMARY_TOTAL_FRAGMENTS = "totalFragments";
  private static final String SUMMARY_TOTAL_DELETION_FILES = "totalDeletionFiles";
  private static final String SUMMARY_TOTAL_DELETION_FILE_ROWS = "totalDeletionFileRows";

  private static final String CHART_TOTAL_RECORDS = "total-records";
  private static final String CHART_TOTAL_DELETE_RECORDS = "total-delete-records";
  private static final String CHART_DATA_FILES = "data-files";
  private static final String CHART_DELETE_FILES = "delete-files";
  private static final String CHART_TOTAL_FILES = "total-files";

  private ExecutorService executor;

  @Override
  public void withIoExecutor(ExecutorService ioExecutor) {
    this.executor = ioExecutor;
  }

  @Override
  public List<TableFormat> supportFormat() {
    return Lists.newArrayList(TableFormat.LANCE);
  }

  @Override
  public ServerTableMeta getTableDetail(AmoroTable<?> amoroTable) {
    Dataset dataset = getDataset(amoroTable);

    ServerTableMeta serverTableMeta = new ServerTableMeta();
    serverTableMeta.setTableIdentifier(amoroTable.id());
    serverTableMeta.setTableType(amoroTable.format().name());

    // 获取 Schema 信息
    Schema schema = dataset.getSchema();
    List<AMSColumnInfo> columns = new ArrayList<>();
    for (Field field : schema.getFields()) {
      columns.add(
          new AMSColumnInfo(
              field.getName(),
              field.getType().toString(),
              !field.isNullable(),
              field.getMetadata() != null ? field.getMetadata().toString() : null));
    }
    serverTableMeta.setSchema(columns);

    // Lance 表没有传统意义上的主键
    serverTableMeta.setPkList(Collections.emptyList());

    // Lance 表没有分区
    serverTableMeta.setPartitionColumnList(Collections.emptyList());

    // 构建 table properties，过滤掉敏感的 COSN 存储配置
    Map<String, String> properties = new HashMap<>(amoroTable.properties());
    LanceProperties.removeSensitiveStorageConfigs(properties);
    properties.put(TABLE_FORMAT, TableFormat.LANCE.name());
    serverTableMeta.setProperties(properties);

    Version currentVersion = dataset.getVersion();
    ManifestSummary summary = currentVersion.getManifestSummary();

    long totalRows = summary.getTotalRows();
    // totalFilesSize 实际是数据文件总大小（Lance 底层拿不到删除文件大小）
    long totalFilesSize = summary.getTotalFilesSize();
    long totalDataFiles = summary.getTotalDataFiles();
    long totalDeletionFiles = summary.getTotalDeletionFiles();
    long totalFragments = summary.getTotalFragments();

    // 总文件数 = 数据文件数 + 删除文件数
    long totalFileCount = totalDataFiles + totalDeletionFiles;
    // 平均文件大小 = 数据文件大小 / 数据文件个数
    long averageFileSize = totalDataFiles > 0 ? totalFilesSize / totalDataFiles : 0;

    // 创建表摘要
    TableSummary tableSummary =
        new TableSummary(
            (int) totalFileCount,
            String.valueOf(totalFilesSize),
            String.valueOf(averageFileSize),
            totalRows,
            TableFormat.LANCE.name());
    serverTableMeta.setTableSummary(tableSummary);

    Map<String, Object> baseMetric = new HashMap<>();
    baseMetric.put(METRIC_VERSION, currentVersion.getId());
    baseMetric.put(METRIC_ROW_COUNT, totalRows);
    baseMetric.put(METRIC_TOTAL_SIZE, totalFilesSize);
    baseMetric.put(METRIC_FILE_COUNT, totalFileCount);
    baseMetric.put(METRIC_DATA_FILE_COUNT, totalDataFiles);
    baseMetric.put(METRIC_DELETION_FILE_COUNT, totalDeletionFiles);
    baseMetric.put(METRIC_FRAGMENT_COUNT, totalFragments);
    baseMetric.put(METRIC_AVERAGE_FILE_SIZE, averageFileSize);

    serverTableMeta.setBaseMetrics(baseMetric);

    return serverTableMeta;
  }

  @Override
  public List<AmoroSnapshotsOfTable> getSnapshots(
      AmoroTable<?> amoroTable, String ref, OperationType operationType) {
    long start = System.currentTimeMillis();
    Dataset dataset = getDataset(amoroTable);
    List<AmoroSnapshotsOfTable> snapshots = new ArrayList<>();

    List<Version> versions = dataset.listVersions();
    for (Version version : versions) {
      ManifestSummary summary = version.getManifestSummary();

      Map<String, String> versionSummary = new HashMap<>();
      versionSummary.put(SUMMARY_VERSION, String.valueOf(version.getId()));
      versionSummary.put(SUMMARY_TOTAL_ROWS, String.valueOf(summary.getTotalRows()));
      versionSummary.put(SUMMARY_TOTAL_DATA_FILES, String.valueOf(summary.getTotalDataFiles()));
      versionSummary.put(SUMMARY_TOTAL_FILES_SIZE, String.valueOf(summary.getTotalFilesSize()));
      versionSummary.put(SUMMARY_TOTAL_FRAGMENTS, String.valueOf(summary.getTotalFragments()));
      versionSummary.put(
          SUMMARY_TOTAL_DELETION_FILES, String.valueOf(summary.getTotalDeletionFiles()));
      versionSummary.put(
          SUMMARY_TOTAL_DELETION_FILE_ROWS, String.valueOf(summary.getTotalDeletionFileRows()));

      // 将 ZonedDateTime 转换为毫秒时间戳
      long commitTime =
          version.getDataTime() != null ? version.getDataTime().toInstant().toEpochMilli() : 0L;

      // 总文件数 = 数据文件数 + 删除文件数
      long totalFileCount = summary.getTotalDataFiles() + summary.getTotalDeletionFiles();

      AmoroSnapshotsOfTable snapshot =
          new AmoroSnapshotsOfTable(
              String.valueOf(version.getId()),
              (int) totalFileCount,
              summary.getTotalFilesSize(),
              summary.getTotalRows(),
              commitTime,
              OPERATION_TYPE_APPEND,
              STORAGE_TYPE_TCLAKE,
              versionSummary);

      Map<String, String> recordsSummaryForChart = new HashMap<>();
      recordsSummaryForChart.put(CHART_TOTAL_RECORDS, String.valueOf(summary.getTotalRows()));
      recordsSummaryForChart.put(
          CHART_TOTAL_DELETE_RECORDS, String.valueOf(summary.getTotalDeletionFileRows()));
      snapshot.setRecordsSummaryForChart(recordsSummaryForChart);

      Map<String, String> filesSummaryForChart = new HashMap<>();
      filesSummaryForChart.put(CHART_DATA_FILES, String.valueOf(summary.getTotalDataFiles()));
      filesSummaryForChart.put(CHART_DELETE_FILES, String.valueOf(summary.getTotalDeletionFiles()));
      filesSummaryForChart.put(CHART_TOTAL_FILES, String.valueOf(totalFileCount));
      snapshot.setFilesSummaryForChart(filesSummaryForChart);

      snapshots.add(snapshot);
    }
    LOG.info(
        "getSnapshots cost: table={}, snapshots={}, elapsed={}ms",
        amoroTable.id(),
        snapshots.size(),
        System.currentTimeMillis() - start);
    return snapshots;
  }

  @Override
  public List<PartitionFileBaseInfo> getSnapshotDetail(
      AmoroTable<?> amoroTable, String snapshotId, String ref) {
    Dataset dataset = getDataset(amoroTable);

    try {
      long versionId = Long.parseLong(snapshotId);
      // 切换到指定版本获取 fragment 文件信息
      try (Dataset versionDataset = dataset.checkoutVersion(versionId)) {
        Version version = versionDataset.getVersion();
        long commitTime =
            version.getDataTime() != null ? version.getDataTime().toInstant().toEpochMilli() : 0L;

        return collectFragmentFiles(versionDataset.getFragments(), snapshotId, commitTime);
      }
    } catch (NumberFormatException e) {
      // snapshotId 解析失败，返回空列表
    }

    return new ArrayList<>();
  }

  @Override
  public List<DDLInfo> getTableOperations(AmoroTable<?> amoroTable) {
    // Lance 暂不支持 DDL 历史
    return Collections.emptyList();
  }

  @Override
  public List<PartitionBaseInfo> getTablePartitions(AmoroTable<?> amoroTable) {
    Dataset dataset = getDataset(amoroTable);

    Version currentVersion = dataset.getVersion();
    long commitTime =
        currentVersion.getDataTime() != null
            ? currentVersion.getDataTime().toInstant().toEpochMilli()
            : 0L;

    List<PartitionBaseInfo> partitions = new ArrayList<>();
    for (Fragment fragment : dataset.getFragments()) {
      FragmentMetadata metadata = fragment.metadata();
      String partitionName = buildFragmentPartitionName(metadata.getId());

      int fileCount = 0;
      long fileSize = 0L;

      // 统计 DataFile
      if (metadata.getFiles() != null) {
        fileCount += metadata.getFiles().size();
        for (DataFile dataFile : metadata.getFiles()) {
          if (dataFile.getFileSizeBytes() != null) {
            fileSize += dataFile.getFileSizeBytes();
          }
        }
      }

      // 统计 DeletionFile
      if (metadata.getDeletionFile() != null) {
        fileCount += 1;
      }

      partitions.add(new PartitionBaseInfo(partitionName, 0, fileCount, fileSize, commitTime));
    }
    return partitions;
  }

  @Override
  public List<PartitionFileBaseInfo> getTableFiles(
      AmoroTable<?> amoroTable, String partition, Integer specId) {
    Dataset dataset = getDataset(amoroTable);

    Version currentVersion = dataset.getVersion();
    long commitTime =
        currentVersion.getDataTime() != null
            ? currentVersion.getDataTime().toInstant().toEpochMilli()
            : 0L;
    String versionId = String.valueOf(currentVersion.getId());

    List<Fragment> fragments = dataset.getFragments();

    // 根据 partition 参数（格式为 "fragment-{id}"）过滤指定的 Fragment
    if (partition != null && !partition.isEmpty() && !"null".equals(partition)) {
      fragments =
          fragments.stream()
              .filter(f -> buildFragmentPartitionName(f.metadata().getId()).equals(partition))
              .collect(Collectors.toList());
    }

    return collectFragmentFiles(fragments, versionId, commitTime);
  }

  @Override
  public Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      AmoroTable<?> amoroTable,
      String type,
      ProcessStatus status,
      int limit,
      int offset,
      String beginTime,
      String endTime) {
    // Lance 暂不支持
    return Pair.of(Collections.emptyList(), 0);
  }

  @Override
  public OptimizingProcessInfo getSingleOptimizingProcessInfo(
      AmoroTable<?> amoroTable, String processId) {
    // Lance 暂不支持
    return new OptimizingProcessInfo();
  }

  @Override
  public Map<String, String> getTableOptimizingTypes(AmoroTable<?> amoroTable) {
    Map<String, String> types = Maps.newHashMap();
    types.put(OPTIMIZING_TYPE_COMPACTION_KEY, OPTIMIZING_TYPE_COMPACTION_VALUE);
    return types;
  }

  @Override
  public List<OptimizingTaskInfo> getOptimizingTaskInfos(
      AmoroTable<?> amoroTable, String processId) {
    // Lance 优化任务信息暂不支持
    return Collections.emptyList();
  }

  @Override
  public List<TagOrBranchInfo> getTableTags(AmoroTable<?> amoroTable) {
    Dataset dataset = getDataset(amoroTable);
    List<TagOrBranchInfo> tagInfos = new ArrayList<>();

    try {
      List<Tag> tags = dataset.tags().list();
      for (Tag tag : tags) {
        tagInfos.add(
            new TagOrBranchInfo(
                tag.getName(),
                tag.getVersion(), // 对应 snapshotId
                -1, // minSnapshotsToKeep，Lance 不支持
                0L, // maxSnapshotAgeMs
                0L, // maxRefAgeMs
                TagOrBranchInfo.TAG));
      }
    } catch (Exception e) {
      // 如果获取 tag 失败，返回空列表
    }

    return tagInfos;
  }

  @Override
  public List<TagOrBranchInfo> getTableBranches(AmoroTable<?> amoroTable) {
    Dataset dataset = getDataset(amoroTable);
    List<TagOrBranchInfo> branchInfos = new ArrayList<>();

    // 始终包含 main 分支
    branchInfos.add(TagOrBranchInfo.MAIN_BRANCH);

    try {
      List<Branch> branches = dataset.branches().list();
      for (Branch branch : branches) {
        // 跳过 main 分支，因为已经添加
        if (BRANCH_MAIN.equals(branch.getName())) {
          continue;
        }
        branchInfos.add(
            new TagOrBranchInfo(
                branch.getName(),
                branch.getParentVersion(), // snapshotId
                -1, // minSnapshotsToKeep
                0L, // maxSnapshotAgeMs
                branch.getCreateAt() * 1000, // ms
                TagOrBranchInfo.BRANCH));
      }
    } catch (Exception e) {
      // 如果获取 branch 失败，仅返回 main 分支
    }

    return branchInfos;
  }

  @Override
  public List<ConsumerInfo> getTableConsumerInfos(AmoroTable<?> amoroTable) {
    // Lance 暂不支持 Consumer
    return Collections.emptyList();
  }

  /**
   * 从 Fragment 列表中收集所有文件信息（DataFile + DeletionFile）。
   *
   * @param fragments Fragment 列表
   * @param versionId 版本标识
   * @param commitTime 提交时间戳（毫秒）
   */
  private List<PartitionFileBaseInfo> collectFragmentFiles(
      List<Fragment> fragments, String versionId, long commitTime) {
    List<PartitionFileBaseInfo> fileInfos = new ArrayList<>();
    for (Fragment fragment : fragments) {
      FragmentMetadata metadata = fragment.metadata();
      String partitionName = buildFragmentPartitionName(metadata.getId());

      // 遍历 Fragment 下的多个 DataFile
      if (metadata.getFiles() != null) {
        for (DataFile dataFile : metadata.getFiles()) {
          long fileSize = dataFile.getFileSizeBytes() != null ? dataFile.getFileSizeBytes() : 0L;
          fileInfos.add(
              new PartitionFileBaseInfo(
                  versionId,
                  FILE_TYPE_DATA,
                  commitTime,
                  partitionName,
                  0,
                  dataFile.getPath(),
                  fileSize));
        }
      }

      // 处理 Fragment 下的 DeletionFile（每个 Fragment 最多一个）
      // Lance deletion 文件命名规则: _deletions/{fragment_id}-{readVersion}-{id}.arrow
      DeletionFile deletionFile = metadata.getDeletionFile();
      if (deletionFile != null) {
        String deletionFileName = buildDeletionFilePath(metadata.getId(), deletionFile);
        fileInfos.add(
            new PartitionFileBaseInfo(
                versionId,
                FILE_TYPE_DELETION_PREFIX + deletionFile.getFileType().name(),
                commitTime,
                partitionName,
                0,
                deletionFileName,
                0L));
      }
    }
    return fileInfos;
  }

  /**
   * 构建 Fragment 分区名称。
   *
   * @param fragmentId Fragment ID
   * @return 格式为 "fragment-{id}" 的分区名
   */
  private static String buildFragmentPartitionName(long fragmentId) {
    return FRAGMENT_PREFIX + fragmentId;
  }

  /**
   * 构建删除文件路径。
   *
   * @param fragmentId Fragment ID
   * @param deletionFile 删除文件对象
   * @return 格式为 "_deletions/{fragment_id}-{readVersion}-{id}.arrow" 的路径
   */
  private static String buildDeletionFilePath(long fragmentId, DeletionFile deletionFile) {
    return String.format(
        DELETION_FILE_PATH_FORMAT, fragmentId, deletionFile.getReadVersion(), deletionFile.getId());
  }

  private Dataset getDataset(AmoroTable<?> amoroTable) {
    return (Dataset) amoroTable.originalTable();
  }
}
