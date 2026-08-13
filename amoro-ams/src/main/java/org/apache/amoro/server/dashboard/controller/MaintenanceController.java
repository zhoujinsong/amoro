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

package org.apache.amoro.server.dashboard.controller;

import io.javalin.http.Context;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.server.dashboard.response.ErrorResponse;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.server.maintenance.ManualMaintenanceService;
import org.apache.amoro.server.maintenance.TriggerResult;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.table.TableIdentifier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 人工运维操作的 Controller。独立于 TableController，专门处理运维相关的 API。 使用 volatile 延迟注入
 * ManualMaintenanceService，因为 optimizingService 在 HTTP 服务启动之后才初始化。
 */
public class MaintenanceController {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceController.class);

  private final TableManager tableManager;
  private volatile ManualMaintenanceService maintenanceService;

  public MaintenanceController(TableManager tableManager) {
    this.tableManager = tableManager;
  }

  /** 设置 ManualMaintenanceService。由 AmoroServiceContainer 在 startOptimizingService() 之后调用。 */
  public void setMaintenanceService(ManualMaintenanceService maintenanceService) {
    this.maintenanceService = maintenanceService;
  }

  /**
   * 手动触发某张表的优化合并。即使 self-optimizing.enabled 为 false 也可以执行。 该接口会将表强制加入调度队列，跳过快照检查和 minPlanInterval
   * 限制。
   *
   * @param ctx - context for handling the request and response
   */
  public void triggerOptimizing(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(db)
            && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");

    // 将 volatile 字段赋值给局部变量，避免 TOCTOU 竞态问题
    ManualMaintenanceService service = this.maintenanceService;
    if (service == null) {
      ctx.json(new ErrorResponse("Optimizing service is not ready yet. Please try again later."));
      return;
    }

    ServerTableIdentifier serverTableIdentifier =
        tableManager.getServerTableIdentifier(
            TableIdentifier.of(catalog, db, table).buildTableIdentifier());
    if (serverTableIdentifier == null) {
      ctx.json(new ErrorResponse(String.format("Table %s.%s.%s not found", catalog, db, table)));
      return;
    }

    TriggerResult result = service.triggerOptimizing(serverTableIdentifier.getId());
    if (!result.isSuccess()) {
      ctx.json(
          new ErrorResponse(
              String.format(
                  "Failed to trigger optimizing for table %s.%s.%s: %s",
                  catalog, db, table, result.getMessage())));
      return;
    }
    ctx.json(OkResponse.ok());
  }
}
