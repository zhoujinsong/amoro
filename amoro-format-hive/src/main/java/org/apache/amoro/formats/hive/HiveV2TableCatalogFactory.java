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

package org.apache.amoro.formats.hive;

import org.apache.amoro.FormatCatalog;
import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.TableFormat;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.DynConstructors;
import org.apache.amoro.utils.IsolatedClassLoader;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;

public class HiveV2TableCatalogFactory implements FormatCatalogFactory {

  @Override
  public FormatCatalog create(
      String name, String metastoreType, Map<String, String> properties, TableMetaStore metaStore) {
    Preconditions.checkArgument(StringUtils.isNotBlank(metastoreType), "metastore type is blank");
    if (properties.containsKey(CatalogMetaProperties.HIVE_METASTORE_JARS)) {
      final IsolatedClassLoader isolatedClassLoader =
          IsolatedClassLoader.buildClassLoader(
              Arrays.asList(properties.get(CatalogMetaProperties.HIVE_METASTORE_JARS)));
      return isolatedClassLoader.withClassLoader(
          cl -> {
            DynConstructors.Ctor<FormatCatalog> ctor =
                DynConstructors.builder()
                    .impl(
                        HiveV2Catalog.class.getName(),
                        String.class,
                        Map.class,
                        TableMetaStore.class,
                        IsolatedClassLoader.class)
                    .build();
            return ctor.newInstance(name, properties, metaStore, isolatedClassLoader);
          },
          RuntimeException.class);
    } else {
      return new HiveV2Catalog(name, properties, metaStore);
    }
  }

  @Override
  public TableFormat format() {
    return TableFormat.HIVE_V2;
  }

  @Override
  public Map<String, String> convertCatalogProperties(
      String catalogName, String metastoreType, Map<String, String> unifiedCatalogProperties) {
    return unifiedCatalogProperties;
  }
}
