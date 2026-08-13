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

package org.apache.amoro.server.catalog.gravitino;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;

import java.util.Map;

/** Minimal fake {@link AmoroTable} used by {@link TestGravitinoUnifiedCatalog}. */
class FakeAmoroTable implements AmoroTable<Object> {

  private final TableIdentifier id;
  private final TableFormat format;

  FakeAmoroTable(TableIdentifier id, TableFormat format) {
    this.id = id;
    this.format = format;
  }

  @Override
  public TableIdentifier id() {
    return id;
  }

  @Override
  public TableFormat format() {
    return format;
  }

  @Override
  public Map<String, String> properties() {
    return Maps.newHashMap();
  }

  @Override
  public Object originalTable() {
    return null;
  }

  @Override
  public TableSnapshot currentSnapshot() {
    return null;
  }
}
