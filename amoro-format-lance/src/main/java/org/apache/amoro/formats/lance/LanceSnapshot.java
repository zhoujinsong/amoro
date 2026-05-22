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

import org.apache.amoro.TableSnapshot;
import org.lance.Dataset;
import org.lance.Version;

/** Lance implementation of TableSnapshot interface using Lance Version. */
public class LanceSnapshot implements TableSnapshot {

  private final Version version;

  public LanceSnapshot(Dataset dataset) {
    this.version = dataset != null ? dataset.getVersion() : null;
  }

  /**
   * Returns the watermark of this snapshot.
   *
   * @return -1
   */
  @Override
  public long watermark() {
    return -1;
  }

  /**
   * Returns the commit time of this snapshot.
   *
   * <p>Uses the dataTime from Lance Version.
   *
   * @return commit time in milliseconds
   */
  @Override
  public long commitTime() {
    if (version == null) {
      return -1;
    }
    // Use the dataTime from Version
    return version.getDataTime() != null
        ? version.getDataTime().toInstant().toEpochMilli()
        : System.currentTimeMillis();
  }

  /**
   * Returns the id of this snapshot
   *
   * @return the version ID as a string
   */
  @Override
  public String id() {
    if (version == null) {
      return null;
    }
    return String.valueOf(version.getId());
  }

  @Override
  public String operation() {
    return "UNKNOWN";
  }

  @Override
  public long schemaId() {
    if (version == null) {
      return -1;
    }
    return version.getId();
  }
}
