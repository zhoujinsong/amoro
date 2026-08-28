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

package org.apache.amoro.spark.tci;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.amoro.TableFormat;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** TciSparkTableFormat only matches table-format=TCI on Spark Table properties. */
public class TestTciSparkTableFormat {

  @Test
  public void formatIsTci() {
    assertEquals(TableFormat.TCI, new TciSparkTableFormat().format());
  }

  @Test
  public void isFormatOf_requiresTableFormatProperty() {
    TciSparkTableFormat fmt = new TciSparkTableFormat();
    assertTrue(fmt.isFormatOf(propsTable(Collections.singletonMap("table-format", "TCI"))));
    assertFalse(fmt.isFormatOf(propsTable(Collections.singletonMap("table-format", "ICEBERG"))));
    assertFalse(fmt.isFormatOf(propsTable(Collections.emptyMap())));
  }

  private static Table propsTable(Map<String, String> props) {
    Map<String, String> copy = new HashMap<>(props);
    return new Table() {
      @Override
      public String name() {
        return "t";
      }

      @Override
      public StructType schema() {
        return new StructType();
      }

      @Override
      public Map<String, String> properties() {
        return copy;
      }

      @Override
      public Set<TableCapability> capabilities() {
        return Collections.emptySet();
      }
    };
  }
}
