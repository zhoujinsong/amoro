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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.amoro.TableFormat;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Ensures TCI is recognized via table-format=TCI and not confused with Iceberg. */
public class TestTciTableFormatRecognition {

  @Test
  public void isTciTableFormat_trueWhenPropertySet() {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogMetaProperties.TABLE_FORMAT, TableFormat.TCI.name());
    assertTrue(CatalogUtil.isTciTableFormat(props));
    assertFalse(CatalogUtil.isIcebergTableFormat(props));
  }

  @Test
  public void isTciTableFormat_falseForIcebergAndEmpty() {
    assertFalse(CatalogUtil.isTciTableFormat(Collections.emptyMap()));
    Map<String, String> iceberg = new HashMap<>();
    iceberg.put(CatalogMetaProperties.TABLE_FORMAT, TableFormat.ICEBERG.name());
    assertFalse(CatalogUtil.isTciTableFormat(iceberg));
    assertTrue(CatalogUtil.isIcebergTableFormat(iceberg));
  }
}
