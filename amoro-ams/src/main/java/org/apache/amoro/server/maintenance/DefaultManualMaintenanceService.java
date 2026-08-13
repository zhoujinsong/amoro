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

import org.apache.amoro.server.DefaultOptimizingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ManualMaintenanceService 的默认实现。 委托给 DefaultOptimizingService 执行实际的优化触发逻辑。 */
public class DefaultManualMaintenanceService implements ManualMaintenanceService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultManualMaintenanceService.class);

  private final DefaultOptimizingService optimizingService;

  public DefaultManualMaintenanceService(DefaultOptimizingService optimizingService) {
    this.optimizingService = optimizingService;
  }

  @Override
  public TriggerResult triggerOptimizing(long tableId) {
    return optimizingService.triggerTableOptimizing(tableId);
  }
}
