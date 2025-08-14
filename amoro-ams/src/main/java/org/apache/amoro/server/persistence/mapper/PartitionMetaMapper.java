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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.server.persistence.PartitionMeta;
import org.apache.amoro.server.persistence.converter.TableFormatConverter;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface PartitionMetaMapper {
  @Update({
    "<script>",
    "REPLACE INTO partition_metadata (table_id, catalog_name, db_name, table_name, part_name, part_name_uniq,"
        + " part_location, snapshot_id, properties, total_records, total_files, total_data_bytes,"
        + " delete_records, delete_files, delete_data_bytes, creator, create_time, modifier, modify_time) "
        + "VALUES ",
    "<foreach collection='parts' item='part' index='index' separator=','>",
    "(#{part.tableId}, #{part.catalogName}, #{part.dbName}, #{part.tableName}, #{part.partName}, #{part.partNameUniq},"
        + " #{part.partLocation}, #{part.snapshotId}, #{part.properties}, #{part.totalRecords}, #{part.totalFiles}, #{part.totalDataBytes},"
        + " #{part.deleteRecords}, #{part.deleteFiles}, #{part.deleteDataBytes}, #{part.creator}, #{part.createTime}, #{part.modifier}, #{part.modifyTime})",
    "</foreach>",
    "</script>"
  })
  long upsertPartitions(@Param("parts") List<PartitionMeta> parts);

  @Delete("DELETE FROM partition_metadata WHERE table_id = #{tableId} limit #{limit}")
  long deletePartitionsByTableId(@Param("tableId") long tableId, @Param("limit") long limit);

  @Delete(
      "DELETE FROM partition_metadata WHERE table_id = #{tableId} and snapshot_id != #{snapshotId} limit #{limit}")
  long deletePartitionsExceptSnapshot(
      @Param("tableId") long tableId,
      @Param("snapshotId") String snapshotId,
      @Param("limit") long limit);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, null as format FROM partition_metadatadata"
          + " WHERE table_id not in (select table_id from table_identifier) limit #{limit}")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format", typeHandler = TableFormatConverter.class)
  })
  List<ServerTableIdentifier> selectNoneExistTables(@Param("limit") long limit);

  @Select(
      "SELECT table_id FROM (SELECT COUNT(DISTINCT current_snapshot_id) AS snapshot_cnt, table_id"
          + " FROM amoro.table_runtime GROUP BY table_id) t WHERE t.snapshot_cnt > 2 #{limit}")
  List<Long> selectTableWithMultiSnapshots(@Param("limit") long limit);
}
