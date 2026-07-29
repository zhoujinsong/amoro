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

  // Fragment partition name prefix
  private static final String FRAGMENT_PREFIX = "fragment-";
  // File type constants
  private static final String FILE_TYPE_DATA = "DATA_FILE";
  private static final String FILE_TYPE_DELETION_PREFIX = "DELETION_FILE_";
  // Deletion file path format
  private static final String DELETION_FILE_PATH_FORMAT = "_deletions/%d-%d-%d.arrow";
  // Optimizing type constants
  private static final String OPTIMIZING_TYPE_COMPACTION_KEY = "COMPACTION";
  private static final String OPTIMIZING_TYPE_COMPACTION_VALUE = "compaction";
  // Branch name constants
  private static final String BRANCH_MAIN = "main";
  // baseMetric key constants
  private static final String METRIC_VERSION = "version";
  private static final String METRIC_ROW_COUNT = "rowCount";
  private static final String METRIC_TOTAL_SIZE = "totalSize";
  private static final String METRIC_FILE_COUNT = "fileCount";
  private static final String METRIC_DATA_FILE_COUNT = "dataFileCount";
  private static final String METRIC_DELETION_FILE_COUNT = "deletionFileCount";
  private static final String METRIC_FRAGMENT_COUNT = "fragmentCount";
  private static final String METRIC_AVERAGE_FILE_SIZE = "averageFileSize";
  // versionSummary key constants
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
    // Empty table (registered in HMS but dataset not yet created): use placeholder to avoid NPE
    if (LanceProperties.isEmptyLanceTable(amoroTable)) {
      return buildEmptyServerTableMeta(amoroTable);
    }
    Dataset dataset = getDataset(amoroTable);
    try {
      ServerTableMeta serverTableMeta = new ServerTableMeta();
      serverTableMeta.setTableIdentifier(amoroTable.id());
      serverTableMeta.setTableType(amoroTable.format().name());

      // Build Schema info
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

      // Lance tables have no primary key
      serverTableMeta.setPkList(Collections.emptyList());

      // Lance tables have no partitions
      serverTableMeta.setPartitionColumnList(Collections.emptyList());

      // Build table properties; filter out sensitive COSN storage configurations
      Map<String, String> properties = new HashMap<>(amoroTable.properties());
      LanceProperties.removeSensitiveStorageConfigs(properties);
      properties.put(TABLE_FORMAT, TableFormat.LANCE.name());
      serverTableMeta.setProperties(properties);

      Version currentVersion = dataset.getVersion();
      ManifestSummary summary = currentVersion.getManifestSummary();

      long totalRows = summary.getTotalRows();
      // totalFilesSize is actually the total size of data files (Lance does not expose deletion
      // file size at the underlying layer)
      long totalFilesSize = summary.getTotalFilesSize();
      long totalDataFiles = summary.getTotalDataFiles();
      long totalDeletionFiles = summary.getTotalDeletionFiles();
      long totalFragments = summary.getTotalFragments();

      // Total file count = data files + deletion files
      long totalFileCount = totalDataFiles + totalDeletionFiles;
      // Average file size = data file total size / number of data files
      long averageFileSize = totalDataFiles > 0 ? totalFilesSize / totalDataFiles : 0;

      // Build table summary
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
    } finally {
      closeDataset(dataset);
    }
  }

  @Override
  public List<AmoroSnapshotsOfTable> getSnapshots(
      AmoroTable<?> amoroTable, String ref, OperationType operationType) {
    long start = System.currentTimeMillis();
    // Empty table has no versions; return an empty list directly
    if (LanceProperties.isEmptyLanceTable(amoroTable)) {
      LOG.info("getSnapshots skipped: empty Lance table {}", amoroTable.id());
      return Collections.emptyList();
    }
    Dataset dataset = getDataset(amoroTable);
    try {
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

        // Convert ZonedDateTime to epoch millis
        long commitTime =
            version.getDataTime() != null ? version.getDataTime().toInstant().toEpochMilli() : 0L;

        // Total file count = data files + deletion files
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
        filesSummaryForChart.put(
            CHART_DELETE_FILES, String.valueOf(summary.getTotalDeletionFiles()));
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
    } finally {
      closeDataset(dataset);
    }
  }

  @Override
  public List<PartitionFileBaseInfo> getSnapshotDetail(
      AmoroTable<?> amoroTable, String snapshotId, String ref) {
    // Empty table has no snapshot details
    if (LanceProperties.isEmptyLanceTable(amoroTable)) {
      return Collections.emptyList();
    }
    Dataset dataset = getDataset(amoroTable);

    try {
      long versionId = Long.parseLong(snapshotId);
      // Switch to the specified version to obtain fragment file info
      try (Dataset versionDataset = dataset.checkoutVersion(versionId)) {
        Version version = versionDataset.getVersion();
        long commitTime =
            version.getDataTime() != null ? version.getDataTime().toInstant().toEpochMilli() : 0L;

        return collectFragmentFiles(versionDataset.getFragments(), snapshotId, commitTime);
      }
    } catch (NumberFormatException e) {
      // Failed to parse snapshotId; return an empty list
    } finally {
      closeDataset(dataset);
    }

    return new ArrayList<>();
  }

  @Override
  public List<DDLInfo> getTableOperations(AmoroTable<?> amoroTable) {
    // Lance does not support DDL history
    return Collections.emptyList();
  }

  @Override
  public List<PartitionBaseInfo> getTablePartitions(AmoroTable<?> amoroTable) {
    // Empty table has no fragments / partitions
    if (LanceProperties.isEmptyLanceTable(amoroTable)) {
      return Collections.emptyList();
    }
    Dataset dataset = getDataset(amoroTable);
    try {
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

        // Count DataFiles
        if (metadata.getFiles() != null) {
          fileCount += metadata.getFiles().size();
          for (DataFile dataFile : metadata.getFiles()) {
            if (dataFile.getFileSizeBytes() != null) {
              fileSize += dataFile.getFileSizeBytes();
            }
          }
        }

        // Count DeletionFile
        if (metadata.getDeletionFile() != null) {
          fileCount += 1;
        }

        partitions.add(new PartitionBaseInfo(partitionName, 0, fileCount, fileSize, commitTime));
      }
      return partitions;
    } finally {
      closeDataset(dataset);
    }
  }

  @Override
  public List<PartitionFileBaseInfo> getTableFiles(
      AmoroTable<?> amoroTable, String partition, Integer specId) {
    // Empty table has no data files
    if (LanceProperties.isEmptyLanceTable(amoroTable)) {
      return Collections.emptyList();
    }
    Dataset dataset = getDataset(amoroTable);
    try {
      Version currentVersion = dataset.getVersion();
      long commitTime =
          currentVersion.getDataTime() != null
              ? currentVersion.getDataTime().toInstant().toEpochMilli()
              : 0L;
      String versionId = String.valueOf(currentVersion.getId());

      List<Fragment> fragments = dataset.getFragments();

      // Filter the specified Fragment by the partition argument (format "fragment-{id}")
      if (partition != null && !partition.isEmpty() && !"null".equals(partition)) {
        fragments =
            fragments.stream()
                .filter(f -> buildFragmentPartitionName(f.metadata().getId()).equals(partition))
                .collect(Collectors.toList());
      }

      return collectFragmentFiles(fragments, versionId, commitTime);
    } finally {
      closeDataset(dataset);
    }
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
    // Not yet supported for Lance
    return Pair.of(Collections.emptyList(), 0);
  }

  @Override
  public OptimizingProcessInfo getSingleOptimizingProcessInfo(
      AmoroTable<?> amoroTable, String processId) {
    // Not yet supported for Lance
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
    // Optimizing task info is not yet supported for Lance
    return Collections.emptyList();
  }

  @Override
  public List<TagOrBranchInfo> getTableTags(AmoroTable<?> amoroTable) {
    // Empty table has no tags
    if (LanceProperties.isEmptyLanceTable(amoroTable)) {
      return Collections.emptyList();
    }
    Dataset dataset = getDataset(amoroTable);
    List<TagOrBranchInfo> tagInfos = new ArrayList<>();

    try {
      List<Tag> tags = dataset.tags().list();
      for (Tag tag : tags) {
        tagInfos.add(
            new TagOrBranchInfo(
                tag.getName(),
                tag.getVersion(), // corresponds to snapshotId
                -1, // minSnapshotsToKeep, not supported by Lance
                0L, // maxSnapshotAgeMs
                0L, // maxRefAgeMs
                TagOrBranchInfo.TAG));
      }
    } catch (Exception e) {
      // If fetching tags fails, return an empty list
    } finally {
      closeDataset(dataset);
    }

    return tagInfos;
  }

  @Override
  public List<TagOrBranchInfo> getTableBranches(AmoroTable<?> amoroTable) {
    // Empty table only returns the default main branch
    if (LanceProperties.isEmptyLanceTable(amoroTable)) {
      List<TagOrBranchInfo> mainOnly = new ArrayList<>();
      mainOnly.add(TagOrBranchInfo.MAIN_BRANCH);
      return mainOnly;
    }
    Dataset dataset = getDataset(amoroTable);
    List<TagOrBranchInfo> branchInfos = new ArrayList<>();

    // Always include the main branch
    branchInfos.add(TagOrBranchInfo.MAIN_BRANCH);

    try {
      List<Branch> branches = dataset.branches().list();
      for (Branch branch : branches) {
        // Skip the main branch since it has already been added
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
      // If fetching branches fails, only the main branch is returned
    } finally {
      closeDataset(dataset);
    }

    return branchInfos;
  }

  @Override
  public List<ConsumerInfo> getTableConsumerInfos(AmoroTable<?> amoroTable) {
    // Lance does not yet support consumers
    return Collections.emptyList();
  }

  /**
   * Collect all file information (DataFile + DeletionFile) from a list of fragments.
   *
   * @param fragments fragment list
   * @param versionId version identifier
   * @param commitTime commit timestamp in millis
   */
  private List<PartitionFileBaseInfo> collectFragmentFiles(
      List<Fragment> fragments, String versionId, long commitTime) {
    List<PartitionFileBaseInfo> fileInfos = new ArrayList<>();
    for (Fragment fragment : fragments) {
      FragmentMetadata metadata = fragment.metadata();
      String partitionName = buildFragmentPartitionName(metadata.getId());

      // Iterate the multiple DataFiles under the fragment
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

      // Handle the DeletionFile under the fragment (at most one per fragment)
      // Lance deletion file naming rule: _deletions/{fragment_id}-{readVersion}-{id}.arrow
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
   * Build the partition name of a fragment.
   *
   * @param fragmentId fragment id
   * @return partition name in the format "fragment-{id}"
   */
  private static String buildFragmentPartitionName(long fragmentId) {
    return FRAGMENT_PREFIX + fragmentId;
  }

  /**
   * Build the deletion file path.
   *
   * @param fragmentId fragment id
   * @param deletionFile deletion file object
   * @return path in the format "_deletions/{fragment_id}-{readVersion}-{id}.arrow"
   */
  private static String buildDeletionFilePath(long fragmentId, DeletionFile deletionFile) {
    return String.format(
        DELETION_FILE_PATH_FORMAT, fragmentId, deletionFile.getReadVersion(), deletionFile.getId());
  }

  private Dataset getDataset(AmoroTable<?> amoroTable) {
    return (Dataset) amoroTable.originalTable();
  }

  /**
   * Close the Lance Dataset to release ArrowBuf and native memory.
   *
   * <p>Lance Dataset is opened without an explicit allocator (see LanceRestCatalog.loadTable /
   * LanceHiveCatalog.loadTable / LanceDirectoryCatalog.loadTable), so the SDK sets
   * selfManagedAllocator=true internally. This means Dataset.close() will release all off-heap
   * memory (ArrowBuf, native pointers) owned by the Dataset. Each loadTable() creates a new
   * Dataset, so it must be closed after use to avoid memory leak.
   */
  private void closeDataset(Dataset dataset) {
    if (dataset == null) {
      return;
    }
    try {
      dataset.close();
    } catch (Exception e) {
      LOG.warn("Failed to close Lance dataset", e);
    }
  }

  /**
   * Build a placeholder {@link ServerTableMeta} for an empty Lance table (registered in HMS but the
   * dataset has not been created yet), so that the table-detail page on the frontend can be opened
   * normally and only shows "no data".
   */
  private ServerTableMeta buildEmptyServerTableMeta(AmoroTable<?> amoroTable) {
    ServerTableMeta serverTableMeta = new ServerTableMeta();
    serverTableMeta.setTableIdentifier(amoroTable.id());
    serverTableMeta.setTableType(amoroTable.format().name());
    serverTableMeta.setSchema(Collections.emptyList());
    serverTableMeta.setPkList(Collections.emptyList());
    serverTableMeta.setPartitionColumnList(Collections.emptyList());

    Map<String, String> properties = new HashMap<>(amoroTable.properties());
    LanceProperties.removeSensitiveStorageConfigs(properties);
    properties.put(TABLE_FORMAT, TableFormat.LANCE.name());
    serverTableMeta.setProperties(properties);

    serverTableMeta.setTableSummary(new TableSummary(0, "0", "0", 0L, TableFormat.LANCE.name()));

    Map<String, Object> baseMetric = new HashMap<>();
    baseMetric.put(METRIC_VERSION, 0L);
    baseMetric.put(METRIC_ROW_COUNT, 0L);
    baseMetric.put(METRIC_TOTAL_SIZE, 0L);
    baseMetric.put(METRIC_FILE_COUNT, 0L);
    baseMetric.put(METRIC_DATA_FILE_COUNT, 0L);
    baseMetric.put(METRIC_DELETION_FILE_COUNT, 0L);
    baseMetric.put(METRIC_FRAGMENT_COUNT, 0L);
    baseMetric.put(METRIC_AVERAGE_FILE_SIZE, 0L);
    serverTableMeta.setBaseMetrics(baseMetric);

    LOG.info("getTableDetail returns empty placeholder for empty Lance table {}", amoroTable.id());
    return serverTableMeta;
  }
}
