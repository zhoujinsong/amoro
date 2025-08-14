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

import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.ExpiredTableMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExpiredTablePersistent extends PersistentBase {

  private static final Logger log = LoggerFactory.getLogger(ExpiredTablePersistent.class);

  public void upsertExpiredTable(ExpiredTable table) {
    doAs(ExpiredTableMapper.class, mapper -> mapper.upsertExpiredTable(table));
  }

  public void deleteExpiredTableWithoutPartition(long tableId) {
    doAs(ExpiredTableMapper.class, mapper -> mapper.deleteExpiredTableWithoutPartition(tableId));
  }

  public List<ExpiredTable> selectExpiredTables(long batchSize) {
    return getAs(ExpiredTableMapper.class, mapper -> mapper.selectExpiredTables(batchSize));
  }
}
