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

import org.apache.amoro.server.utils.MD5Util;
import org.junit.Assert;
import org.junit.Test;

public class TestMD5Util {

  @Test
  public void testMD5() {
    String source =
        "DataLakeCatalog.test_schema.test_table.dt=20250806/appid=% s x a @&*-+cosn://dlc8db7-100018379117-1695115817-100017307912-1304028854/1305424723/warehouse/mjunli_test.db/test_iceberg_part.1753267599281";
    String expected = "f0e6df9cdcf12210798b46090388645a";
    Assert.assertEquals(expected, MD5Util.md5(source));

    source = "a";
    expected = "0cc175b9c0f1b6a831c399e269772661";
    Assert.assertEquals(expected, MD5Util.md5(source));

    source = "";
    expected = "d41d8cd98f00b204e9800998ecf8427e";
    Assert.assertEquals(expected, MD5Util.md5(source));
  }
}
