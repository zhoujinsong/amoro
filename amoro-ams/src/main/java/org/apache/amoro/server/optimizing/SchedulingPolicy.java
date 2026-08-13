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

package org.apache.amoro.server.optimizing;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.optimizing.sorter.QuotaOccupySorter;
import org.apache.amoro.server.optimizing.sorter.SorterFactory;
import org.apache.amoro.server.table.DefaultOptimizingState;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SchedulingPolicy {

  public static final Logger LOG = LoggerFactory.getLogger(SchedulingPolicy.class);

  private static final String SCHEDULING_POLICY_PROPERTY_NAME = "scheduling-policy";

  private final Map<ServerTableIdentifier, DefaultTableRuntime> tableRuntimeMap = new HashMap<>();
  // 强制触发优化的表集合，被标记的表跳过快照检查和minPlanInterval限制
  private final Set<ServerTableIdentifier> forceTriggerTables = ConcurrentHashMap.newKeySet();
  private volatile String policyName;
  private final Lock tableLock = new ReentrantLock();
  private static final Map<String, SorterFactory> sorterFactoryCache = new ConcurrentHashMap<>();

  public SchedulingPolicy(ResourceGroup group) {
    setTableSorterIfNeeded(group);
  }

  public void setTableSorterIfNeeded(ResourceGroup optimizerGroup) {
    tableLock.lock();
    try {
      policyName =
          Optional.ofNullable(optimizerGroup.getProperties())
              .orElseGet(Maps::newHashMap)
              .getOrDefault(SCHEDULING_POLICY_PROPERTY_NAME, QuotaOccupySorter.IDENTIFIER);
    } finally {
      tableLock.unlock();
    }
  }

  static {
    ServiceLoader<SorterFactory> sorterFactories = ServiceLoader.load(SorterFactory.class);
    Iterator<SorterFactory> iterator = sorterFactories.iterator();
    iterator.forEachRemaining(
        sorterFactory -> {
          String identifier = sorterFactory.getIdentifier();
          sorterFactoryCache.put(identifier, sorterFactory);
          LOG.info(
              "Loaded scheduling policy {} and its corresponding sorter instance {}",
              identifier,
              sorterFactory.getClass().getName());
        });
  }

  public String name() {
    return policyName;
  }

  public DefaultTableRuntime scheduleTable(Set<ServerTableIdentifier> skipSet) {
    tableLock.lock();
    try {
      fillSkipSet(skipSet);
      // 优先选中强制触发的表，直接遍历forceTriggerTables（通常只有0~几个元素），
      // 避免遍历全量tableRuntimeMap带来的性能开销
      for (ServerTableIdentifier identifier : forceTriggerTables) {
        if (!skipSet.contains(identifier)) {
          DefaultTableRuntime tableRuntime = tableRuntimeMap.get(identifier);
          if (tableRuntime != null) {
            // 表被选中后才消费标记
            forceTriggerTables.remove(identifier);
            return tableRuntime;
          }
        }
      }
      return tableRuntimeMap.values().stream()
          .filter(tableRuntime -> !skipSet.contains(tableRuntime.getTableIdentifier()))
          .min(createSorterByPolicy())
          .orElse(null);
    } finally {
      tableLock.unlock();
    }
  }

  private Comparator<DefaultTableRuntime> createSorterByPolicy() {
    if (sorterFactoryCache.get(policyName) != null) {
      SorterFactory sorterFactory = sorterFactoryCache.get(policyName);
      LOG.debug(
          "Using sorter instance {} corresponding to the scheduling policy {}",
          sorterFactory.getClass().getName(),
          policyName);
      return sorterFactory.createComparator();
    } else {
      throw new IllegalArgumentException("Unsupported scheduling policy: " + policyName);
    }
  }

  public DefaultTableRuntime getTableRuntime(ServerTableIdentifier tableIdentifier) {
    tableLock.lock();
    try {
      return tableRuntimeMap.get(tableIdentifier);
    } finally {
      tableLock.unlock();
    }
  }

  /** 添加强制触发标记，被标记的表将跳过快照检查和minPlanInterval限制。 标记为一次性使用，表被选中调度后自动移除。 */
  public void addForceTrigger(ServerTableIdentifier identifier) {
    forceTriggerTables.add(identifier);
  }

  /** 检查是否有强制触发的表等待调度 */
  public boolean hasForceTriggerTables() {
    return !forceTriggerTables.isEmpty();
  }

  /** 检查指定表是否已经在强制触发的等待队列中 */
  public boolean isForceTriggerPending(ServerTableIdentifier identifier) {
    return forceTriggerTables.contains(identifier);
  }

  private void fillSkipSet(Set<ServerTableIdentifier> originalSet) {
    long currentTime = System.currentTimeMillis();
    tableRuntimeMap.values().stream()
        .map(DefaultTableRuntime::getOptimizingState)
        .filter(
            optimizingState -> {
              // 强制触发的表不加入跳过集合（即不跳过，一定会被调度）
              // 注意：这里只检查不消费，标记在scheduleTable选中表后才消费
              if (forceTriggerTables.contains(optimizingState.getTableIdentifier())) {
                return false;
              }
              return !isTablePending(optimizingState)
                  || currentTime - optimizingState.getLastPlanTime()
                      < optimizingState.getOptimizingConfig().getMinPlanInterval();
            })
        .forEach(tableRuntime -> originalSet.add(tableRuntime.getTableIdentifier()));
  }

  private boolean isTablePending(DefaultOptimizingState optimizingState) {
    return optimizingState.getOptimizingStatus() == OptimizingStatus.PENDING
        && (optimizingState.getLastOptimizedSnapshotId() != optimizingState.getCurrentSnapshotId()
            || optimizingState.getLastOptimizedChangeSnapshotId()
                != optimizingState.getCurrentChangeSnapshotId());
  }

  public void addTable(DefaultTableRuntime tableRuntime) {
    tableLock.lock();
    try {
      tableRuntimeMap.put(tableRuntime.getTableIdentifier(), tableRuntime);
    } finally {
      tableLock.unlock();
    }
  }

  public void removeTable(DefaultTableRuntime tableRuntime) {
    tableLock.lock();
    try {
      tableRuntimeMap.remove(tableRuntime.getTableIdentifier());
    } finally {
      tableLock.unlock();
    }
  }

  @VisibleForTesting
  Map<ServerTableIdentifier, DefaultTableRuntime> getTableRuntimeMap() {
    return tableRuntimeMap;
  }
}
