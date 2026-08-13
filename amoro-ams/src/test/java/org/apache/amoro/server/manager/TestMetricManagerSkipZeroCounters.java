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

package org.apache.amoro.server.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MetricManager#mergeGlobalSkipZeroCounters} that lock the X2 priority
 * contract documented in §2.2 / §3.2 of
 * docs/软件设计/2026-05-29-AMS指标体系精简/2026-06-02-按值过滤Counter零值指标.md:
 *
 * <ul>
 *   <li>T3.6 — global {@code true} + reporter properties without the key → properties get injected
 *       as {@code "true"};
 *   <li>T3.7 — local {@code "false"} + global {@code true} → properties keep {@code "false"} (local
 *       wins, putIfAbsent semantics).
 * </ul>
 */
public class TestMetricManagerSkipZeroCounters {

  private static final String NAME_PROM = "prometheus-exporter";
  private static final String NAME_OTHER = "other-reporter";
  private static final String KEY = "skip-zero-counters";

  private static PluginConfiguration prom(Map<String, String> props) {
    return new PluginConfiguration(NAME_PROM, true, props);
  }

  /** T3.6 — global true is propagated when the local key is absent. */
  @Test
  public void testGlobalInjectedWhenLocalAbsent() {
    Map<String, String> props = new HashMap<>();
    props.put("port", "7001");

    List<PluginConfiguration> merged =
        MetricManager.mergeGlobalSkipZeroCounters(Collections.singletonList(prom(props)), true);

    assertEquals(1, merged.size());
    assertEquals("true", merged.get(0).getProperties().get(KEY));
    assertEquals("7001", merged.get(0).getProperties().get("port"));
  }

  /** T3.6.b — global false is propagated when the local key is absent. */
  @Test
  public void testGlobalFalseInjectedWhenLocalAbsent() {
    Map<String, String> props = new HashMap<>();
    props.put("port", "7001");

    List<PluginConfiguration> merged =
        MetricManager.mergeGlobalSkipZeroCounters(Collections.singletonList(prom(props)), false);

    assertEquals("false", merged.get(0).getProperties().get(KEY));
  }

  /** T3.7 — explicit local false wins over global true. */
  @Test
  public void testLocalFalseWinsOverGlobalTrue() {
    Map<String, String> props = new HashMap<>();
    props.put("port", "7001");
    props.put(KEY, "false");

    List<PluginConfiguration> merged =
        MetricManager.mergeGlobalSkipZeroCounters(Collections.singletonList(prom(props)), true);

    assertEquals("false", merged.get(0).getProperties().get(KEY));
  }

  /** Non-prometheus reporter properties must remain untouched. */
  @Test
  public void testOtherReporterIsUntouched() {
    Map<String, String> props = new HashMap<>();
    props.put("foo", "bar");
    PluginConfiguration other = new PluginConfiguration(NAME_OTHER, true, props);

    List<PluginConfiguration> merged =
        MetricManager.mergeGlobalSkipZeroCounters(Collections.singletonList(other), true);

    assertEquals(1, merged.size());
    assertEquals(NAME_OTHER, merged.get(0).getName());
    assertEquals(false, merged.get(0).getProperties().containsKey(KEY));
  }
}
