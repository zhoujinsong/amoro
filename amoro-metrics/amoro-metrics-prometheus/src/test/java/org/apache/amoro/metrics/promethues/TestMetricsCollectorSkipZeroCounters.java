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

package org.apache.amoro.metrics.promethues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.prometheus.client.Collector.MetricFamilySamples;
import org.apache.amoro.metrics.Counter;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MetricsCollector#createFamilySample}-driven scrape-stage filtering of
 * Counter samples whose current value equals 0.
 *
 * <p>Test matrix is anchored to the design contract in
 * docs/软件设计/2026-05-29-AMS指标体系精简/2026-06-02-按值过滤Counter零值指标.md §4 Task 3.
 */
public class TestMetricsCollectorSkipZeroCounters {

  private static final MetricDefine COUNTER_DEFINE =
      MetricDefine.defineCounter("test_counter").withTags("name").build();

  private static final MetricDefine GAUGE_DEFINE =
      MetricDefine.defineGauge("test_gauge").withTags("name").build();

  private static MetricKey counterKey(String name) {
    Map<String, String> tagValues = new LinkedHashMap<>();
    tagValues.put("name", name);
    return new MetricKey(COUNTER_DEFINE, tagValues);
  }

  private static MetricKey gaugeKey(String name) {
    Map<String, String> tagValues = new LinkedHashMap<>();
    tagValues.put("name", name);
    return new MetricKey(GAUGE_DEFINE, tagValues);
  }

  private static Counter counter(long value) {
    Counter c = new Counter();
    if (value > 0) {
      c.inc(value);
    }
    return c;
  }

  private static Gauge<Long> gauge(long value) {
    return () -> value;
  }

  private static MetricSet metricSetOf(Map<MetricKey, Metric> entries) {
    return () -> entries;
  }

  private static List<MetricFamilySamples.Sample> samplesOf(
      List<MetricFamilySamples> families, String fullName) {
    return families.stream()
        .filter(f -> f.name.equals(fullName))
        .findFirst()
        .map(f -> f.samples)
        .orElseThrow(() -> new AssertionError("metric family not found: " + fullName));
  }

  /** T3.1 default behaviour: 0 / 0 / 5 -> all 3 samples are emitted. */
  @Test
  public void testDefaultBehaviour_KeepsZeroCounters() {
    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(counterKey("a"), counter(0));
    entries.put(counterKey("b"), counter(0));
    entries.put(counterKey("c"), counter(5));

    List<MetricFamilySamples> families = new MetricsCollector(metricSetOf(entries)).collect();
    List<MetricFamilySamples.Sample> samples = samplesOf(families, "amoro_test_counter");

    assertEquals(3, samples.size(), "default constructor must keep all samples");
  }

  /** T3.2 enabled: 0 / 0 / 5 -> only the value=5 sample remains. */
  @Test
  public void testEnabled_DropsZeroCounters() {
    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(counterKey("a"), counter(0));
    entries.put(counterKey("b"), counter(0));
    entries.put(counterKey("c"), counter(5));

    List<MetricFamilySamples> families = new MetricsCollector(metricSetOf(entries), true).collect();
    List<MetricFamilySamples.Sample> samples = samplesOf(families, "amoro_test_counter");

    assertEquals(1, samples.size());
    assertEquals(5.0d, samples.get(0).value, 0.0d);
    assertTrue(samples.get(0).labelValues.contains("c"));
  }

  /** T3.3 Gauge is never affected by skipZeroCounters (only skipZeroGauges controls Gauge). */
  @Test
  public void testEnabled_GaugeSamplesAreUnaffected() {
    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(gaugeKey("a"), gauge(0L));
    entries.put(gaugeKey("b"), gauge(0L));
    entries.put(gaugeKey("c"), gauge(5L));

    List<MetricFamilySamples> families = new MetricsCollector(metricSetOf(entries), true).collect();
    List<MetricFamilySamples.Sample> samples = samplesOf(families, "amoro_test_gauge");

    assertEquals(3, samples.size(), "Gauge samples must never be filtered by skipZeroCounters");
  }

  /** T3.5 skipZeroGauges enabled: 0 / 0 / 5 -> only the value=5 Gauge sample remains. */
  @Test
  public void testSkipZeroGaugesEnabled_DropsZeroGauges() {
    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(gaugeKey("a"), gauge(0L));
    entries.put(gaugeKey("b"), gauge(0L));
    entries.put(gaugeKey("c"), gauge(5L));

    List<MetricFamilySamples> families =
        new MetricsCollector(metricSetOf(entries), false, true).collect();
    List<MetricFamilySamples.Sample> samples = samplesOf(families, "amoro_test_gauge");

    assertEquals(1, samples.size());
    assertEquals(5.0d, samples.get(0).value, 0.0d);
    assertTrue(samples.get(0).labelValues.contains("c"));
  }

  /** T3.6 skipZeroGauges does not affect Counter samples. */
  @Test
  public void testSkipZeroGaugesEnabled_CounterSamplesAreUnaffected() {
    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(counterKey("a"), counter(0));
    entries.put(counterKey("b"), counter(0));
    entries.put(counterKey("c"), counter(5));

    List<MetricFamilySamples> families =
        new MetricsCollector(metricSetOf(entries), false, true).collect();
    List<MetricFamilySamples.Sample> samples = samplesOf(families, "amoro_test_counter");

    assertEquals(3, samples.size(), "Counter samples must never be filtered by skipZeroGauges");
  }

  /** T3.7 both skipZeroCounters and skipZeroGauges enabled: both types are filtered. */
  @Test
  public void testBothEnabled_DropsZeroCountersAndGauges() {
    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(counterKey("a"), counter(0));
    entries.put(counterKey("b"), counter(3));
    entries.put(gaugeKey("x"), gauge(0L));
    entries.put(gaugeKey("y"), gauge(7L));

    List<MetricFamilySamples> families =
        new MetricsCollector(metricSetOf(entries), true, true).collect();

    List<MetricFamilySamples.Sample> counterSamples = samplesOf(families, "amoro_test_counter");
    assertEquals(1, counterSamples.size());
    assertEquals(3.0d, counterSamples.get(0).value, 0.0d);

    List<MetricFamilySamples.Sample> gaugeSamples = samplesOf(families, "amoro_test_gauge");
    assertEquals(1, gaugeSamples.size());
    assertEquals(7.0d, gaugeSamples.get(0).value, 0.0d);
  }

  /** T3.8 skipZeroGauges: Gauge recovers when value transitions from 0 to non-zero. */
  @Test
  public void testSkipZeroGauges_RecoversWhenGaugeBecomesNonZero() {
    // 使用一个可变的 Gauge
    long[] holder = {0L};
    Gauge<Long> mutableGauge = () -> holder[0];

    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(gaugeKey("a"), mutableGauge);

    MetricsCollector collector = new MetricsCollector(metricSetOf(entries), false, true);

    // 第一次采集: value == 0 -> 被过滤
    List<MetricFamilySamples.Sample> first = samplesOf(collector.collect(), "amoro_test_gauge");
    assertEquals(0, first.size());

    // 值变为非零
    holder[0] = 42L;

    // 第二次采集: value == 42 -> 输出
    List<MetricFamilySamples.Sample> second = samplesOf(collector.collect(), "amoro_test_gauge");
    assertEquals(1, second.size());
    assertEquals(42.0d, second.get(0).value, 0.0d);
  }

  /** T3.4 incremental recovery: previously zero counter recovers after inc(). */
  @Test
  public void testEnabled_RecoversWhenCounterIncrements() {
    Counter c = counter(0);
    Map<MetricKey, Metric> entries = new HashMap<>();
    entries.put(counterKey("a"), c);

    MetricsCollector collector = new MetricsCollector(metricSetOf(entries), true);

    // first scrape: value == 0 -> filtered out
    List<MetricFamilySamples.Sample> first = samplesOf(collector.collect(), "amoro_test_counter");
    assertEquals(0, first.size());

    c.inc();
    // second scrape: value == 1 -> emitted
    List<MetricFamilySamples.Sample> second = samplesOf(collector.collect(), "amoro_test_counter");
    assertEquals(1, second.size());
    assertEquals(1.0d, second.get(0).value, 0.0d);
  }
}
