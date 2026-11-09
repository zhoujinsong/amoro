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

package org.apache.amoro.server.catalog.gravitino;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableMetaStore;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Concurrency regression tests for {@link GravitinoUnifiedCatalog}.
 *
 * <p>Production incident (AMS jstack, 2026-11-05): {@code refresh()} held the instance monitor
 * while {@code invalidateAll()} walked Caffeine's hash bins, and a cache loader held one of those
 * bins while {@code gravitinoCatalog()} waited for the instance monitor. The resulting ABBA
 * deadlock froze table discovery and every table-level scheduler forever.
 */
public class TestGravitinoUnifiedCatalogConcurrency {

  private static final String CATALOG_NAME = "test_gravitino_catalog";
  private static final String DB = "db1";
  private static final TableMetaStore META_STORE = TableMetaStore.builder().build();
  private static final long STEP_TIMEOUT_MS = 10_000L;

  private GravitinoClient client;
  private TableCatalog tableCatalog;

  @Before
  public void setUp() {
    client = mock(GravitinoClient.class);
    Catalog gravitinoCatalog = mock(Catalog.class);
    tableCatalog = mock(TableCatalog.class);
    lenient().when(client.loadCatalog(anyString())).thenReturn(gravitinoCatalog);
    lenient().when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);
    lenient().when(gravitinoCatalog.properties()).thenReturn(Maps.newHashMap());
    Table missingTable = tableWithFormat(null);
    lenient().when(tableCatalog.loadTable(any())).thenReturn(missingTable);
  }

  /**
   * Deterministically replays the interleaving captured in the production jstack:
   *
   * <ol>
   *   <li>Loader C is inside a cache loader for a key whose hash bin is the first one visited by
   *       {@code Cache#invalidateAll()} (simulating a slow Gravitino call).
   *   <li>Refresher A calls {@code refresh()}: takes the instance monitor, drops the cached
   *       Gravitino catalog handle and starts clearing the cache; it parks on C's bin.
   *   <li>Loader B starts a cache loader on the last bin visited by {@code invalidateAll()}; inside
   *       the loader it needs the (now dropped) catalog handle and asks for the instance monitor.
   *   <li>C finishes; A advances to B's bin. A holds monitor and waits bin, B holds bin and waits
   *       monitor: deadlock.
   * </ol>
   *
   * <p>Bin placement relies on {@code "Aa".hashCode() == "BB".hashCode()}, so an existing entry and
   * a loading key always share one {@link ConcurrentHashMap} bin, forcing the loader to lock the
   * existing node that {@code invalidateAll()} must remove.
   */
  @Test(timeout = 60_000)
  public void testRefreshRacingCacheLoaderDoesNotDeadlock() throws Exception {
    String firstBin = prefixForBinLowBits(0);
    String lastBin = prefixForBinLowBits(1023);
    String firstBinExisting = firstBin + "Aa";
    String firstBinLoading = firstBin + "BB";
    String lastBinExisting = lastBin + "Aa";
    String lastBinLoading = lastBin + "BB";

    CountDownLatch loaderCInside = new CountDownLatch(1);
    CountDownLatch releaseLoaderC = new CountDownLatch(1);
    Table icebergTable = tableWithFormat("iceberg");
    when(tableCatalog.loadTable(ident(firstBinLoading)))
        .thenAnswer(
            invocation -> {
              loaderCInside.countDown();
              releaseLoaderC.await(STEP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
              return icebergTable;
            });

    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());
    // Seed one entry in each of the two bins.
    catalog.tableExists(DB, firstBinExisting);
    catalog.tableExists(DB, lastBinExisting);

    Thread loaderC = startDaemon("loader-C", () -> catalog.tableExists(DB, firstBinLoading));
    assertTrue(
        "loader C never entered the cache loader", loaderCInside.await(10, TimeUnit.SECONDS));

    Thread refresherA = startDaemon("refresher-A", catalog::refresh);
    awaitParkedOrTerminated(refresherA);

    Thread loaderB = startDaemon("loader-B", () -> catalog.tableExists(DB, lastBinLoading));
    awaitParkedOrTerminated(loaderB);

    releaseLoaderC.countDown();

    assertAllFinish(Lists.newArrayList(refresherA, loaderB, loaderC), 5_000L);
  }

  /** Randomized smoke test: many loaders plus a refresher hammering the same catalog instance. */
  @Test(timeout = 60_000)
  public void testConcurrentLoadsAndRefreshesMakeProgress() throws Exception {
    Table icebergTable = tableWithFormat("iceberg");
    lenient()
        .when(tableCatalog.loadTable(any()))
        .thenAnswer(
            invocation -> {
              // Mimic a remote round-trip so loaders overlap with refresh().
              Thread.sleep(ThreadLocalRandom.current().nextInt(2));
              return icebergTable;
            });
    GravitinoUnifiedCatalog catalog = newCatalog(Maps.newHashMap());

    AtomicBoolean stop = new AtomicBoolean(false);
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < 8; i++) {
      threads.add(
          startDaemon(
              "loader-" + i,
              () -> {
                while (!stop.get()) {
                  catalog.loadTableFormat(DB, "t" + ThreadLocalRandom.current().nextInt(32));
                }
              }));
    }
    threads.add(
        startDaemon(
            "refresher",
            () -> {
              while (!stop.get()) {
                catalog.refresh();
              }
            }));

    Thread.sleep(2_000L);
    stop.set(true);
    assertAllFinish(threads, 5_000L);
  }

  // -----------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------

  private GravitinoUnifiedCatalog newCatalog(Map<String, String> extraProperties) {
    Map<String, String> props = Maps.newHashMap();
    props.put(GravitinoUnifiedCatalogProperties.GRAVITINO_URI, "http://localhost:8090");
    props.put(GravitinoUnifiedCatalogProperties.WAREHOUSE, "metalake1.lakehouse1");
    props.putAll(extraProperties);
    return new GravitinoUnifiedCatalog(CATALOG_NAME, props, META_STORE, client);
  }

  private static Table tableWithFormat(String format) {
    Table table = mock(Table.class);
    Map<String, String> props = Maps.newHashMap();
    if (format != null) {
      props.put("format", format);
    }
    lenient().when(table.properties()).thenReturn(props);
    return table;
  }

  private static NameIdentifier ident(String table) {
    return NameIdentifier.of(Namespace.of(DB), table);
  }

  /** Mirrors {@code ConcurrentHashMap.spread}, which picks the bin from the key hash. */
  private static int spread(int h) {
    return (h ^ (h >>> 16)) & 0x7fffffff;
  }

  /**
   * Finds a table-name prefix whose cache key ({@code "<db>.<table>"}) lands in a bin with the
   * given low 10 hash bits. With low bits 0 / 1023 the bin is the first / last one iterated for any
   * table size up to 1024, which covers the handful of entries created by the test.
   */
  private static String prefixForBinLowBits(int lowBits) {
    for (int i = 0; ; i++) {
      String prefix = "t" + i + "_";
      if ((spread((DB + "." + prefix + "Aa").hashCode()) & 1023) == lowBits) {
        return prefix;
      }
    }
  }

  private static Thread startDaemon(String name, Runnable body) {
    Thread thread = new Thread(body, name);
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /** Waits until the thread is parked on a lock/condition (or already done). */
  private static void awaitParkedOrTerminated(Thread thread) throws InterruptedException {
    long deadline = System.currentTimeMillis() + STEP_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      Thread.State state = thread.getState();
      if (state == Thread.State.BLOCKED
          || state == Thread.State.WAITING
          || state == Thread.State.TERMINATED) {
        return;
      }
      Thread.sleep(5);
    }
    fail("Thread " + thread.getName() + " neither parked nor finished, state=" + thread.getState());
  }

  private static void assertAllFinish(List<Thread> threads, long timeoutMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    for (Thread thread : threads) {
      thread.join(Math.max(1L, deadline - System.currentTimeMillis()));
    }
    List<Thread> stuck = threads.stream().filter(Thread::isAlive).collect(Collectors.toList());
    if (stuck.isEmpty()) {
      return;
    }
    fail(describeStuckThreads(stuck));
  }

  private static String describeStuckThreads(List<Thread> stuck) {
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
    Set<Long> stuckIds = stuck.stream().map(Thread::getId).collect(Collectors.toSet());
    long[] deadlocked = mxBean.findDeadlockedThreads();
    boolean deadlock = deadlocked != null && Arrays.stream(deadlocked).anyMatch(stuckIds::contains);
    StringBuilder sb =
        new StringBuilder(deadlock ? "DEADLOCK detected" : "Threads did not finish")
            .append(" among ")
            .append(stuck.stream().map(Thread::getName).collect(Collectors.toList()))
            .append('\n');
    for (ThreadInfo info :
        mxBean.getThreadInfo(stuckIds.stream().mapToLong(Long::longValue).toArray(), true, true)) {
      if (info == null) {
        continue;
      }
      sb.append('"')
          .append(info.getThreadName())
          .append("\" ")
          .append(info.getThreadState())
          .append(" on ")
          .append(info.getLockName())
          .append(" owned by ")
          .append(info.getLockOwnerName())
          .append('\n');
      Arrays.stream(info.getStackTrace())
          .limit(12)
          .forEach(frame -> sb.append("\tat ").append(frame).append('\n'));
    }
    return sb.toString();
  }
}
