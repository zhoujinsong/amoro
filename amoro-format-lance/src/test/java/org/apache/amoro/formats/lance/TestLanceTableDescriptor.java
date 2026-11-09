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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.utils.CommonUtil;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lance.Dataset;
import org.lance.ListVersionsOptions;
import org.lance.ManifestSummary;
import org.lance.Version;
import org.lance.WriteParams;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Unit tests for {@link LanceTableDescriptor}. */
public class TestLanceTableDescriptor {

  private static final String POOL_THREAD_PREFIX = "lance-descriptor-test-";

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private ExecutorService executor;
  private LanceTableDescriptor descriptor;
  private Dataset dataset;

  @Before
  public void setUp() {
    AtomicInteger threadIdx = new AtomicInteger();
    executor =
        Executors.newFixedThreadPool(
            4, r -> new Thread(r, POOL_THREAD_PREFIX + threadIdx.getAndIncrement()));
    descriptor = new LanceTableDescriptor();
    descriptor.withIoExecutor(executor);
    dataset = mock(Dataset.class);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void testGetSnapshotsSortedByVersionDescWithExecutor() {
    List<Version> versions = versionsOf(2, 5, 1, 4, 3);
    when(dataset.listVersions()).thenReturn(versions);

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(Arrays.asList("5", "4", "3", "2", "1"), snapshotIds(snapshots));
    verify(dataset, times(1)).close();
  }

  @Test
  public void testGetSnapshotsSortedByVersionDescWithoutExecutor() {
    LanceTableDescriptor serialDescriptor = new LanceTableDescriptor();
    List<Version> versions = versionsOf(3, 1, 2);
    when(dataset.listVersions()).thenReturn(versions);

    List<AmoroSnapshotsOfTable> snapshots =
        serialDescriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(Arrays.asList("3", "2", "1"), snapshotIds(snapshots));
    verify(dataset, times(1)).close();
  }

  @Test
  public void testGetSnapshotsLoadedInParallelOnExecutor() {
    int versionCount = 4;
    // Every version blocks until all versions have started; a serial implementation would time out
    CountDownLatch allStarted = new CountDownLatch(versionCount);
    Set<String> threadNames = ConcurrentHashMap.newKeySet();
    List<Version> versions = new ArrayList<>();
    for (int i = 1; i <= versionCount; i++) {
      ManifestSummary summary = mockSummary(i);
      Version version = mock(Version.class);
      when(version.getId()).thenReturn((long) i);
      when(version.getDataTime()).thenReturn(null);
      when(version.getManifestSummary())
          .thenAnswer(
              invocation -> {
                threadNames.add(Thread.currentThread().getName());
                allStarted.countDown();
                assertTrue(
                    "versions should be loaded concurrently",
                    allStarted.await(10, TimeUnit.SECONDS));
                return summary;
              });
      versions.add(version);
    }
    when(dataset.listVersions()).thenReturn(versions);

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(Arrays.asList("4", "3", "2", "1"), snapshotIds(snapshots));
    assertEquals(versionCount, threadNames.size());
    for (String name : threadNames) {
      assertTrue(
          "should run on the injected executor: " + name, name.startsWith(POOL_THREAD_PREFIX));
    }
  }

  @Test
  public void testGetSnapshotsFieldsMapping() {
    long commitMillis = 1_700_000_000_000L;
    ManifestSummary summary = mock(ManifestSummary.class);
    when(summary.getTotalRows()).thenReturn(1000L);
    when(summary.getTotalDataFiles()).thenReturn(10L);
    when(summary.getTotalDeletionFiles()).thenReturn(2L);
    when(summary.getTotalFilesSize()).thenReturn(4096L);
    when(summary.getTotalFragments()).thenReturn(5L);
    when(summary.getTotalDeletionFileRows()).thenReturn(30L);
    Version version = mock(Version.class);
    when(version.getId()).thenReturn(7L);
    when(version.getDataTime())
        .thenReturn(ZonedDateTime.ofInstant(Instant.ofEpochMilli(commitMillis), ZoneOffset.UTC));
    when(version.getManifestSummary()).thenReturn(summary);
    when(dataset.listVersions()).thenReturn(Collections.singletonList(version));

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(1, snapshots.size());
    AmoroSnapshotsOfTable snapshot = snapshots.get(0);
    assertEquals("7", snapshot.getSnapshotId());
    assertEquals(12, snapshot.getFileCount());
    assertEquals(4096L, snapshot.getOriginalFileSize());
    assertEquals(1000L, snapshot.getRecords());
    assertEquals(commitMillis, snapshot.getCommitTime());
    assertEquals("append", snapshot.getOperation());
    assertEquals("TCLAKE", snapshot.getProducer());

    Map<String, String> summaryMap = snapshot.getSummary();
    assertEquals("7", summaryMap.get("version"));
    assertEquals("1000", summaryMap.get("total-records"));
    assertEquals("10", summaryMap.get("total-data-files"));
    assertEquals(CommonUtil.byteToXB(4096L), summaryMap.get("total-files-size"));
    assertTrue(summaryMap.get("total-files-size").endsWith("KB"));
    assertEquals("5", summaryMap.get("total-fragments"));
    assertEquals("2", summaryMap.get("total-delete-files"));
    assertEquals("30", summaryMap.get("total-deletion-file-rows"));
    for (String key : summaryMap.keySet()) {
      assertTrue("summary key should be hyphen-separated: " + key, key.matches("[a-z]+(-[a-z]+)*"));
    }

    assertEquals("1000", snapshot.getRecordsSummaryForChart().get("total-records"));
    assertEquals("30", snapshot.getRecordsSummaryForChart().get("total-delete-records"));
    assertEquals("10", snapshot.getFilesSummaryForChart().get("data-files"));
    assertEquals("2", snapshot.getFilesSummaryForChart().get("delete-files"));
    assertEquals("12", snapshot.getFilesSummaryForChart().get("total-files"));
  }

  @Test
  public void testGetSnapshotsNullDataTimeFallsBackToZero() {
    List<Version> versions = versionsOf(1);
    when(dataset.listVersions()).thenReturn(versions);

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(0L, snapshots.get(0).getCommitTime());
  }

  @Test
  public void testGetSnapshotsNoVersionsReturnsEmptyList() {
    when(dataset.listVersions()).thenReturn(Collections.emptyList());

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertTrue(snapshots.isEmpty());
    verify(dataset, times(1)).close();
  }

  @Test
  public void testGetSnapshotsEmptyLanceTableSkipsLoading() {
    Map<String, String> properties = new HashMap<>();
    properties.put(LanceProperties.TABLE_EMPTY, "true");
    AmoroTable<Dataset> table = mockTable(dataset, properties);

    List<AmoroSnapshotsOfTable> snapshots = descriptor.getSnapshots(table, null, OperationType.ALL);

    assertTrue(snapshots.isEmpty());
    verify(dataset, never()).listVersions();
    verify(dataset, never()).close();
  }

  @Test
  public void testGetSnapshotsLoadFailurePropagatesCauseAndClosesDataset() {
    IllegalStateException failure = new IllegalStateException("manifest broken");
    Version good = mockVersion(1);
    Version bad = mock(Version.class);
    when(bad.getId()).thenReturn(2L);
    when(bad.getManifestSummary()).thenThrow(failure);
    when(dataset.listVersions()).thenReturn(Arrays.asList(good, bad));

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL));

    assertSame(failure, thrown.getCause());
    verify(dataset, times(1)).close();
  }

  @Test
  public void testGetSnapshotsDoesNotMutateListVersionsResult() {
    List<Version> original = versionsOf(1, 3, 2);
    List<Version> unmodifiable = Collections.unmodifiableList(original);
    when(dataset.listVersions()).thenReturn(unmodifiable);

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(Arrays.asList("3", "2", "1"), snapshotIds(snapshots));
    assertEquals(
        Arrays.asList(1L, 3L, 2L),
        original.stream().map(Version::getId).collect(Collectors.toList()));
  }

  @Test
  public void testGetSnapshotsIndexCountOnlyOnLatestSnapshot() {
    List<Version> versions = versionsOf(1, 3, 2);
    when(dataset.listVersions()).thenReturn(versions);
    when(dataset.version()).thenReturn(3L);
    // Segments of the same index share a name; system indexes must be excluded
    when(dataset.listIndexes())
        .thenReturn(
            Arrays.asList(
                "vec_idx", "vec_idx", "id_btree", "__lance_frag_reuse", "__lance_mem_wal"));

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(Arrays.asList("3", "2", "1"), snapshotIds(snapshots));
    assertEquals("2", snapshots.get(0).getSummary().get("total-indexes"));
    assertFalse(snapshots.get(1).getSummary().containsKey("total-indexes"));
    assertFalse(snapshots.get(2).getSummary().containsKey("total-indexes"));
    verify(dataset, times(1)).listIndexes();
  }

  @Test
  public void testGetSnapshotsIndexCountZeroWhenNoIndex() {
    List<Version> versions = versionsOf(1);
    when(dataset.listVersions()).thenReturn(versions);
    when(dataset.version()).thenReturn(1L);
    when(dataset.listIndexes()).thenReturn(Collections.emptyList());

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals("0", snapshots.get(0).getSummary().get("total-indexes"));
  }

  @Test
  public void testGetSnapshotsIndexCountSkippedWhenDatasetNotAtLatestVersion() {
    List<Version> versions = versionsOf(1, 2);
    when(dataset.listVersions()).thenReturn(versions);
    when(dataset.version()).thenReturn(1L);

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    for (AmoroSnapshotsOfTable snapshot : snapshots) {
      assertFalse(snapshot.getSummary().containsKey("total-indexes"));
    }
    verify(dataset, never()).listIndexes();
  }

  @Test
  public void testGetSnapshotsIndexLoadFailureDoesNotFailRequest() {
    List<Version> versions = versionsOf(1, 2);
    when(dataset.listVersions()).thenReturn(versions);
    when(dataset.version()).thenReturn(2L);
    when(dataset.listIndexes()).thenThrow(new IllegalStateException("index manifest broken"));

    List<AmoroSnapshotsOfTable> snapshots =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL);

    assertEquals(Arrays.asList("2", "1"), snapshotIds(snapshots));
    assertFalse(snapshots.get(0).getSummary().containsKey("total-indexes"));
    verify(dataset, times(1)).close();
  }

  @Test
  public void testGetSnapshotsPagePassesPaginationToLance() {
    List<Version> versions = versionsOf(30, 29);
    when(dataset.getVersionCount()).thenReturn(50L);
    when(dataset.listVersions(any(ListVersionsOptions.class))).thenReturn(versions);

    Pair<List<AmoroSnapshotsOfTable>, Integer> page =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL, 2, 20);

    ArgumentCaptor<ListVersionsOptions> options =
        ArgumentCaptor.forClass(ListVersionsOptions.class);
    verify(dataset).listVersions(options.capture());
    assertTrue(options.getValue().isDescending());
    assertEquals(20L, options.getValue().getOffset());
    assertEquals(Optional.of(2L), options.getValue().getLimit());
    assertEquals(Arrays.asList("30", "29"), snapshotIds(page.getLeft()));
    assertEquals(50, (int) page.getRight());
    verify(dataset, never()).listVersions();
    verify(dataset, never()).listIndexes();
    verify(dataset, times(1)).close();
  }

  @Test
  public void testGetSnapshotsFirstPageAttachesIndexCount() {
    List<Version> versions = versionsOf(3, 2);
    when(dataset.getVersionCount()).thenReturn(3L);
    when(dataset.listVersions(any(ListVersionsOptions.class))).thenReturn(versions);
    when(dataset.version()).thenReturn(3L);
    when(dataset.listIndexes()).thenReturn(Arrays.asList("vec_idx", "__lance_frag_reuse"));

    Pair<List<AmoroSnapshotsOfTable>, Integer> page =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL, 2, 0);

    assertEquals(Arrays.asList("3", "2"), snapshotIds(page.getLeft()));
    assertEquals("1", page.getLeft().get(0).getSummary().get("total-indexes"));
    assertFalse(page.getLeft().get(1).getSummary().containsKey("total-indexes"));
    assertEquals(3, (int) page.getRight());
  }

  @Test
  public void testGetSnapshotsPageBeyondTotalSkipsListing() {
    when(dataset.getVersionCount()).thenReturn(3L);

    Pair<List<AmoroSnapshotsOfTable>, Integer> outOfRange =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL, 20, 20);
    Pair<List<AmoroSnapshotsOfTable>, Integer> zeroLimit =
        descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL, 0, 0);

    assertTrue(outOfRange.getLeft().isEmpty());
    assertEquals(3, (int) outOfRange.getRight());
    assertTrue(zeroLimit.getLeft().isEmpty());
    assertEquals(3, (int) zeroLimit.getRight());
    verify(dataset, never()).listVersions(any(ListVersionsOptions.class));
    verify(dataset, times(2)).close();
  }

  @Test
  public void testGetSnapshotsPageEmptyLanceTableSkipsLoading() {
    Map<String, String> properties = new HashMap<>();
    properties.put(LanceProperties.TABLE_EMPTY, "true");

    Pair<List<AmoroSnapshotsOfTable>, Integer> page =
        descriptor.getSnapshots(mockTable(dataset, properties), null, OperationType.ALL, 20, 0);

    assertTrue(page.getLeft().isEmpty());
    assertEquals(0, (int) page.getRight());
    verify(dataset, never()).getVersionCount();
    verify(dataset, never()).close();
  }

  @Test
  public void testGetSnapshotsPageClosesDatasetOnFailure() {
    when(dataset.getVersionCount()).thenThrow(new IllegalStateException("list failed"));

    assertThrows(
        IllegalStateException.class,
        () -> descriptor.getSnapshots(mockTable(dataset), null, OperationType.ALL, 20, 0));

    verify(dataset, times(1)).close();
  }

  @Test
  public void testGetSnapshotsPageOnRealDataset() throws Exception {
    String uri = tempFolder.newFolder("paged.lance").getAbsolutePath();
    Schema schema =
        new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      for (int i = 0; i < 5; i++) {
        WriteParams.WriteMode mode =
            i == 0 ? WriteParams.WriteMode.CREATE : WriteParams.WriteMode.OVERWRITE;
        Dataset.write().allocator(allocator).uri(uri).schema(schema).mode(mode).execute().close();
      }
    }

    Pair<List<AmoroSnapshotsOfTable>, Integer> first =
        descriptor.getSnapshots(mockTable(Dataset.open(uri)), null, OperationType.ALL, 2, 0);
    Pair<List<AmoroSnapshotsOfTable>, Integer> last =
        descriptor.getSnapshots(mockTable(Dataset.open(uri)), null, OperationType.ALL, 2, 4);
    Pair<List<AmoroSnapshotsOfTable>, Integer> beyond =
        descriptor.getSnapshots(mockTable(Dataset.open(uri)), null, OperationType.ALL, 2, 6);

    assertEquals(Arrays.asList("5", "4"), snapshotIds(first.getLeft()));
    assertEquals(5, (int) first.getRight());
    assertEquals(Collections.singletonList("1"), snapshotIds(last.getLeft()));
    assertEquals(5, (int) last.getRight());
    assertTrue(beyond.getLeft().isEmpty());
    assertEquals(5, (int) beyond.getRight());
  }

  @Test
  public void testGetSnapshotsPageWalkCoversAllVersions() throws Exception {
    int versionCount = 30;
    int pageSize = 7;
    String uri = tempFolder.newFolder("paged_walk.lance").getAbsolutePath();
    Schema schema =
        new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      for (int i = 0; i < versionCount; i++) {
        WriteParams.WriteMode mode =
            i == 0 ? WriteParams.WriteMode.CREATE : WriteParams.WriteMode.OVERWRITE;
        Dataset.write().allocator(allocator).uri(uri).schema(schema).mode(mode).execute().close();
      }
    }

    List<String> walked = new ArrayList<>();
    for (int offset = 0; offset < versionCount; offset += pageSize) {
      Pair<List<AmoroSnapshotsOfTable>, Integer> page =
          descriptor.getSnapshots(
              mockTable(Dataset.open(uri)), null, OperationType.ALL, pageSize, offset);
      assertEquals(versionCount, (int) page.getRight());
      assertTrue(page.getLeft().size() <= pageSize);
      walked.addAll(snapshotIds(page.getLeft()));
    }

    List<String> expected = new ArrayList<>();
    for (int id = versionCount; id >= 1; id--) {
      expected.add(String.valueOf(id));
    }
    assertEquals(expected, walked);
  }

  private static List<String> snapshotIds(List<AmoroSnapshotsOfTable> snapshots) {
    return snapshots.stream()
        .map(AmoroSnapshotsOfTable::getSnapshotId)
        .collect(Collectors.toList());
  }

  private static List<Version> versionsOf(long... ids) {
    List<Version> versions = new ArrayList<>();
    for (long id : ids) {
      versions.add(mockVersion(id));
    }
    return versions;
  }

  private static Version mockVersion(long id) {
    Version version = mock(Version.class);
    when(version.getId()).thenReturn(id);
    when(version.getDataTime()).thenReturn(null);
    ManifestSummary summary = mockSummary(id);
    when(version.getManifestSummary()).thenReturn(summary);
    return version;
  }

  private static ManifestSummary mockSummary(long seed) {
    ManifestSummary summary = mock(ManifestSummary.class);
    when(summary.getTotalRows()).thenReturn(seed * 100);
    when(summary.getTotalDataFiles()).thenReturn(seed);
    when(summary.getTotalDeletionFiles()).thenReturn(0L);
    when(summary.getTotalFilesSize()).thenReturn(seed * 1024);
    when(summary.getTotalFragments()).thenReturn(seed);
    when(summary.getTotalDeletionFileRows()).thenReturn(0L);
    return summary;
  }

  private static AmoroTable<Dataset> mockTable(Dataset dataset) {
    return mockTable(dataset, new HashMap<>());
  }

  @SuppressWarnings("unchecked")
  private static AmoroTable<Dataset> mockTable(Dataset dataset, Map<String, String> properties) {
    AmoroTable<Dataset> table = mock(AmoroTable.class);
    when(table.originalTable()).thenReturn(dataset);
    when(table.properties()).thenReturn(properties);
    when(table.format()).thenReturn(TableFormat.LANCE);
    when(table.id()).thenReturn(TableIdentifier.of("catalog", "db", "tbl"));
    return table;
  }
}
