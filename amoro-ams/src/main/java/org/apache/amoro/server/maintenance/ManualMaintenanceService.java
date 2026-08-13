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

package org.apache.amoro.server.maintenance;

/** 人工运维服务接口。提供手动触发优化合并等运维操作的能力。 这些操作独立于自动调度逻辑，即使 self-optimizing.enabled 为 false 也可以执行。 */
public interface ManualMaintenanceService {

  /**
   * 手动触发某张表的优化合并。即使 self-optimizing.enabled 为 false 也可以执行。 该方法会将表强制加入调度队列，跳过快照检查和 minPlanInterval
   * 限制。
   *
   * @param tableId 表的 ID
   * @return TriggerResult 触发结果，包含成功/失败的具体原因
   */
  TriggerResult triggerOptimizing(long tableId);
}
