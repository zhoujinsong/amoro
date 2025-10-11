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

package org.apache.amoro.utils;

import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HiveUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveUtil.class);

  public static Database constructHiveDatabase(
      String databaseName, Map<String, String> meta, Configuration conf) {
    Database database = new Database();
    Map<String, String> parameter = Maps.newHashMap();
    database.setName(databaseName);
    database.setLocationUri(databaseLocation(conf, databaseName));
    meta.forEach(
        (key, value) -> {
          if (key.equals("comment")) {
            database.setDescription(value);
          } else if (key.equals("location")) {
            database.setLocationUri(value);
          } else if (key.equals("hive.metastore.database.owner")) {
            database.setOwnerName(value);
          } else if (key.equals("hive.metastore.database.owner-type") && value != null) {
            database.setOwnerType(PrincipalType.valueOf(value));
          } else if (value != null) {
            parameter.put(key, value);
          }
        });
    if (database.getOwnerName() == null) {
      database.setOwnerName(currentUser());
      database.setOwnerType(PrincipalType.USER);
    }

    database.setParameters(parameter);
    return database;
  }

  private static String databaseLocation(Configuration conf, String databaseName) {
    String warehouseLocation = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
    return String.format("%s/%s.db", warehouseLocation, databaseName);
  }

  public static String currentUser() {
    String username = null;

    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException var2) {
      LOG.warn("Failed to get Hadoop user", var2);
    }

    if (username != null) {
      return username;
    } else {
      LOG.warn("Hadoop user is null, defaulting to user.name");
      return System.getProperty("user.name");
    }
  }

  public static Map<String, String> collectDatabaseMetadata(Database database) {
    Map<String, String> meta = Maps.newHashMap();
    meta.putAll(database.getParameters());
    meta.put("location", database.getLocationUri());
    if (database.getDescription() != null) {
      meta.put("comment", database.getDescription());
    }

    if (database.getOwnerName() != null) {
      meta.put("hive.metastore.database.owner", database.getOwnerName());
      if (database.getOwnerType() != null) {
        meta.put("hive.metastore.database.owner-type", database.getOwnerType().name());
      }
    }

    return meta;
  }
}
