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

package org.apache.amoro.formats.iceberg;

import org.apache.amoro.FormatCatalog;
import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.TableFormat;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.MixedFormatCatalogUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for {@link TciCatalog}: Iceberg-backed recognition of {@code table-format=TCI} tables
 * only. Does not embed the TCI engine; Spark routes I/O via the pluggable {@code
 * tci.spark-catalog-impl} (offline default spark-tci-stream). Passes {@code tci-stream.config.path}
 * when {@code TCI_STREAM_CONF_PATH} is set for engines that read that option.
 */
public class TciCatalogFactory implements FormatCatalogFactory {

  @Override
  public FormatCatalog create(
      String name, String metastoreType, Map<String, String> properties, TableMetaStore metaStore) {
    Preconditions.checkArgument(StringUtils.isNotBlank(metastoreType), "metastore type is blank");
    properties =
        MixedFormatCatalogUtil.withIcebergCatalogInitializeProperties(
            name, metastoreType, properties);

    Catalog icebergCatalog =
        CatalogUtil.buildIcebergCatalog(name, properties, metaStore.getConfiguration());
    return new TciCatalog(icebergCatalog, properties, metaStore);
  }

  @Override
  public TableFormat format() {
    return TableFormat.TCI;
  }

  @Override
  public Map<String, String> convertCatalogProperties(
      String catalogName, String metastoreType, Map<String, String> unifiedCatalogProperties) {
    Map<String, String> properties =
        new HashMap<>(
            MixedFormatCatalogUtil.withIcebergCatalogInitializeProperties(
                catalogName, metastoreType, unifiedCatalogProperties));
    // Keep unified "uri" as the Iceberg/Hive metastore URI for FormatCatalog recognition.
    // Do NOT overwrite it with tci.uri (TCI server address) — that breaks HMS clients.
    // spark-tci-stream reads the TCI server from client conf (tci-stream.config.path) and
    // HMS from GlobalConfig; only forward conf-path / nested tci.* for the Spark delegate.
    String tciUri =
        firstNonBlank(
            unifiedCatalogProperties.get("tci.uri"), unifiedCatalogProperties.get("tci-uri"));
    if (StringUtils.isNotBlank(tciUri)) {
      properties.put("tci.uri", tciUri);
    }
    String tciConfPath =
        firstNonBlank(
            unifiedCatalogProperties.get("tci.conf-path"),
            unifiedCatalogProperties.get("tci-conf-path"),
            unifiedCatalogProperties.get("tci-stream.config.path"),
            System.getenv("TCI_STREAM_CONF_PATH"));
    if (StringUtils.isNotBlank(tciConfPath)) {
      properties.put("conf-path", tciConfPath);
      properties.put("tci-stream.config.path", tciConfPath);
    }
    return properties;
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (StringUtils.isNotBlank(v)) {
        return v;
      }
    }
    return null;
  }
}
