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

package org.apache.amoro.server.persistence.mapper;

import org.apache.amoro.server.table.ExpiredTable;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface ExpiredTableMapper {

  @Update(
      "REPLACE INTO expired_table (table_id, catalog_name, db_name, table_name, status) VALUE"
          + "(#{table.tableId}, #{table.catalog}, #{table.schema}, #{table.table}, #{table.status})")
  void upsertExpiredTable(@Param("table") ExpiredTable table);

  @Delete(
      "DELETE FROM expired_table WHERE table_id = #{tableId}"
          + " AND #{tableId} NOT IN (SELECT table_id FROM partition_metadata)")
  void deleteExpiredTableWithoutPartition(@Param("tableId") long tableId);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, status FROM expired_table limit #{limit}")
  @Results({
    @Result(property = "tableId", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "schema", column = "db_name"),
    @Result(property = "table", column = "table_name"),
    @Result(property = "status", column = "status")
  })
  List<ExpiredTable> selectExpiredTables(@Param("limit") long limit);
}
