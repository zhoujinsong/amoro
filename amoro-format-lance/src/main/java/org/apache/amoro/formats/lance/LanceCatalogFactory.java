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

package org.apache.amoro.formats.lance;

import org.apache.amoro.FormatCatalog;
import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.TableFormat;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableMetaStore;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.hive3.Hive3Namespace;
import org.lance.namespace.hive3.Hive3NamespaceConfig;

import java.util.Map;

public class LanceCatalogFactory implements FormatCatalogFactory {

  static {
    LanceNamespace.registerNamespaceImpl("hive3", "org.lance.namespace.hive3.Hive3Namespace");
  }

  @Override
  public FormatCatalog create(
      String catalogName,
      String metastoreType,
      Map<String, String> properties,
      TableMetaStore metaStore) {
    Map<String, String> lanceCatalogProperties = Maps.newHashMap(properties);
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    String warehouse = lanceCatalogProperties.get(CatalogMetaProperties.KEY_WAREHOUSE);
    if (warehouse != null && !warehouse.isEmpty()) {
      lanceCatalogProperties.putIfAbsent(
          Hive3NamespaceConfig.ROOT, LanceProperties.normalizeRootUrl(warehouse));
    }

    if (CatalogMetaProperties.CATALOG_TYPE_HADOOP.equalsIgnoreCase(metastoreType)) {
      LanceNamespace namespace = LanceNamespace.connect("dir", lanceCatalogProperties, allocator);
      return new LanceDirectoryCatalog(namespace, lanceCatalogProperties, allocator);
    } else if (CatalogMetaProperties.CATALOG_TYPE_HIVE.equalsIgnoreCase(metastoreType)) {
      Hive3Namespace namespace = new Hive3Namespace();
      namespace.setHadoopConf(metaStore.getConfiguration());
      namespace.initialize(lanceCatalogProperties, allocator);
      return new LanceHiveCatalog(
          catalogName, namespace, lanceCatalogProperties, allocator, metaStore.getConfiguration());
    }
    throw new IllegalArgumentException(
        "Un-supported metastore type:" + metastoreType + " for Lance");
  }

  @Override
  public TableFormat format() {
    return TableFormat.LANCE;
  }

  @Override
  public Map<String, String> convertCatalogProperties(
      String catalogName, String metastoreType, Map<String, String> unifiedCatalogProperties) {
    return unifiedCatalogProperties;
  }
}
