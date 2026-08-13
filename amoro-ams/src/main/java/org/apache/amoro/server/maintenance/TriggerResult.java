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

/** 手动触发优化的结果枚举。 */
public enum TriggerResult {
  /** 成功触发 */
  SUCCESS("Optimizing triggered successfully"),
  /** 表已经在强制触发的等待队列中（重复触发） */
  ALREADY_QUEUED("The table is already queued for optimizing, please wait"),
  /** 表正在优化中 */
  ALREADY_OPTIMIZING("The table is already in optimizing process"),
  /** 表不存在 */
  TABLE_NOT_FOUND("Table not found"),
  /** Optimizer Group 不存在 */
  GROUP_NOT_FOUND("Optimizer group not found or not available");

  private final String message;

  TriggerResult(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public boolean isSuccess() {
    return this == SUCCESS;
  }
}
