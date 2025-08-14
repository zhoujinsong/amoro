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

package org.apache.amoro.server.persistence;

import org.apache.amoro.server.utils.MD5Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionMeta implements AmoroPatitionable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionMeta.class);
  private static final int PART_NAME_LENGTH_MAX = 256;

  private long tableId;
  private final String catalogName;
  private final String dbName;
  private final String tableName;
  private final String partName;
  private final String partNameUniq;
  private final String partLocation;
  private final String snapshotId;
  private String properties;
  private long totalRecords;
  private long totalFiles;
  private long totalDataBytes;
  private long deleteRecords;
  private long deleteFiles;
  private long deleteDataBytes;
  private String creator;
  private long createTime;
  private String modifier;
  private long modifyTime;

  public PartitionMeta(
      String catalogName,
      String dbName,
      String tableName,
      String partName,
      String partLocation,
      String snapshotId) {
    this.catalogName = catalogName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.partName = partName;
    this.partNameUniq = shortPartName(partName);
    this.partLocation = partLocation;
    this.snapshotId = snapshotId;
  }

  private String shortPartName(String partName) {
    if (partName == null) {
      throw new IllegalArgumentException("partName is null");
    }
    if (partName.length() <= PART_NAME_LENGTH_MAX) {
      return partName;
    } else {
      String md5 = MD5Util.md5(partName);
      LOG.debug(
          "Partition:{} has {} characters more than threshold:{}, short it with md5:{}",
          partName,
          partName.length(),
          PART_NAME_LENGTH_MAX,
          md5);
      return partName.substring(0, PART_NAME_LENGTH_MAX - md5.length()) + md5;
    }
  }

  public void setTableId(long tableId) {
    this.tableId = tableId;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public void setDeleteRecords(long deleteRecords) {
    this.deleteRecords = deleteRecords;
  }

  public void setTotalFiles(long totalFiles) {
    this.totalFiles = totalFiles;
  }

  public void setDeleteFiles(long deleteFiles) {
    this.deleteFiles = deleteFiles;
  }

  public void setDeleteDataBytes(long deleteDataBytes) {
    this.deleteDataBytes = deleteDataBytes;
  }

  public void setTotalDataBytes(long totalDataBytes) {
    this.totalDataBytes = totalDataBytes;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public void setModifier(String modifier) {
    this.modifier = modifier;
  }

  public void setModifyTime(long modifyTime) {
    this.modifyTime = modifyTime;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("PartitionMeta{");
    sb.append("tableId=").append(tableId);
    sb.append(", catalogName='").append(catalogName).append('\'');
    sb.append(", dbName='").append(dbName).append('\'');
    sb.append(", tableName='").append(tableName).append('\'');
    sb.append(", partName='").append(partName).append('\'');
    sb.append(", partNameUniq='").append(partNameUniq).append('\'');
    sb.append(", partLocation='").append(partLocation).append('\'');
    sb.append(", snapshotId='").append(snapshotId).append('\'');
    sb.append(", properties='").append(properties).append('\'');
    sb.append(", totalRecords=").append(totalRecords);
    sb.append(", deleteRecords=").append(deleteRecords);
    sb.append(", totalFiles=").append(totalFiles);
    sb.append(", deleteFiles=").append(deleteFiles);
    sb.append(", totalDataBytes=").append(totalDataBytes);
    sb.append(", deleteDataBytes=").append(deleteDataBytes);
    sb.append(", creator=").append(creator);
    sb.append(", createTime=").append(createTime);
    sb.append(", modifier=").append(modifier);
    sb.append(", modifyTime=").append(modifyTime);
    sb.append('}');
    return sb.toString();
  }
}
