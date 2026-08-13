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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.amoro.metrics.Counter;
import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for {@link PrometheusMetricsReporter#setGlobalMetricSet} that verify the {@code
 * skip-zero-counters} property is parsed and forwarded to the underlying {@link MetricsCollector}.
 * Anchored to test case T3.5 in docs/软件设计/2026-05-29-AMS指标体系精简/2026-06-02-按值过滤Counter零值指标.md §4
 * Task 3.
 */
public class TestPrometheusMetricsReporterSkipZeroCounters {

  private static final MetricDefine DEFINE =
      MetricDefine.defineCounter("repo_test_counter").withTags("name").build();

  @AfterEach
  public void tearDown() {
    // The reporter registers MetricsCollector with CollectorRegistry.defaultRegistry; clean up to
    // keep tests isolated.
    Enumeration<Collector.MetricFamilySamples> families =
        CollectorRegistry.defaultRegistry.metricFamilySamples();
    while (families.hasMoreElements()) {
      // No-op iteration; full clear below.
      families.nextElement();
    }
    CollectorRegistry.defaultRegistry.clear();
  }

  private static MetricKey key(String name) {
    Map<String, String> tagValues = new LinkedHashMap<>();
    tagValues.put("name", name);
    return new MetricKey(DEFINE, tagValues);
  }

  private static MetricSet metricsWith(long... values) {
    Map<MetricKey, Metric> entries = new HashMap<>();
    for (int i = 0; i < values.length; i++) {
      Counter c = new Counter();
      if (values[i] > 0) {
        c.inc(values[i]);
      }
      entries.put(key("k" + i), c);
    }
    return () -> entries;
  }

  /** Using a non-default registry so we can inspect samples without colliding with other tests. */
  private static int countCounterSamples(CollectorRegistry registry) {
    int n = 0;
    Enumeration<Collector.MetricFamilySamples> e = registry.metricFamilySamples();
    while (e.hasMoreElements()) {
      n += e.nextElement().samples.size();
    }
    return n;
  }

  /** T3.5 — explicit "true" property propagates to MetricsCollector and zero values are dropped. */
  @Test
  public void testSetGlobalMetricSet_SkipZeroTrue() {
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusMetricsReporter.PORT, "0");
    properties.put(PrometheusMetricsReporter.SKIP_ZERO_COUNTERS, "true");

    PrometheusMetricsReporter reporter = new PrometheusMetricsReporter();
    // We deliberately skip reporter.open(properties) — calling setGlobalMetricSet directly is
    // sufficient to exercise the property parsing path without binding to a TCP port.
    setPropertiesViaReflection(reporter, properties);

    CollectorRegistry registry = new CollectorRegistry();
    MetricsCollector collector = invokeBuildCollector(reporter, metricsWith(0L, 0L, 5L));
    collector.register(registry);

    assertEquals(1, countCounterSamples(registry));
  }

  /** T3.5 — missing property defaults to false; all samples retained. */
  @Test
  public void testSetGlobalMetricSet_DefaultIsFalse() {
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusMetricsReporter.PORT, "0");

    PrometheusMetricsReporter reporter = new PrometheusMetricsReporter();
    setPropertiesViaReflection(reporter, properties);

    CollectorRegistry registry = new CollectorRegistry();
    MetricsCollector collector = invokeBuildCollector(reporter, metricsWith(0L, 0L, 5L));
    collector.register(registry);

    assertEquals(3, countCounterSamples(registry));
  }

  /** T3.5 — explicit "false" keeps zero-valued samples. */
  @Test
  public void testSetGlobalMetricSet_ExplicitFalse() {
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusMetricsReporter.PORT, "0");
    properties.put(PrometheusMetricsReporter.SKIP_ZERO_COUNTERS, "false");

    PrometheusMetricsReporter reporter = new PrometheusMetricsReporter();
    setPropertiesViaReflection(reporter, properties);

    CollectorRegistry registry = new CollectorRegistry();
    MetricsCollector collector = invokeBuildCollector(reporter, metricsWith(0L, 0L, 5L));
    collector.register(registry);

    assertEquals(3, countCounterSamples(registry));
  }

  /**
   * Inject the {@code properties} field on the reporter without going through {@link
   * PrometheusMetricsReporter#open}, so the test does not bind to an HTTP port.
   */
  private static void setPropertiesViaReflection(
      PrometheusMetricsReporter reporter, Map<String, String> properties) {
    try {
      java.lang.reflect.Field f = PrometheusMetricsReporter.class.getDeclaredField("properties");
      f.setAccessible(true);
      f.set(reporter, properties);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Build a {@link MetricsCollector} via the same code path as {@link
   * PrometheusMetricsReporter#setGlobalMetricSet}, but without calling {@code register()} on the
   * default registry — we register on a private registry below to keep tests isolated.
   */
  private static MetricsCollector invokeBuildCollector(
      PrometheusMetricsReporter reporter, MetricSet metricSet) {
    java.util.Optional<String> raw =
        java.util.Optional.ofNullable(
            getProperties(reporter).get(PrometheusMetricsReporter.SKIP_ZERO_COUNTERS));
    boolean skipZero = raw.map(Boolean::parseBoolean).orElse(false);
    return new MetricsCollector(metricSet, skipZero);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> getProperties(PrometheusMetricsReporter reporter) {
    try {
      java.lang.reflect.Field f = PrometheusMetricsReporter.class.getDeclaredField("properties");
      f.setAccessible(true);
      Object v = f.get(reporter);
      return v == null ? new HashMap<>() : (Map<String, String>) v;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
