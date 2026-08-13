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

import io.prometheus.client.exporter.HTTPServer;
import org.apache.amoro.metrics.MetricReporter;
import org.apache.amoro.metrics.MetricSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** Prometheus exporter */
public class PrometheusMetricsReporter implements MetricReporter {

  public static final String PORT = "port";

  /**
   * Property key controlling scrape-stage filtering of zero-valued Counter samples. When set to
   * {@code "true"}, the {@link MetricsCollector} drops Counter samples whose current value equals 0
   * from the {@code /metrics} output. Defaults to {@code false} when absent.
   */
  public static final String SKIP_ZERO_COUNTERS = "skip-zero-counters";

  /**
   * Property key controlling scrape-stage filtering of zero-valued Gauge samples. When set to
   * {@code "true"}, the {@link MetricsCollector} drops Gauge samples whose current value equals 0
   * from the {@code /metrics} output. Defaults to {@code false} when absent.
   */
  public static final String SKIP_ZERO_GAUGES = "skip-zero-gauges";

  private HTTPServer server;
  private Map<String, String> properties = Collections.emptyMap();

  @Override
  public void open(Map<String, String> properties) {
    this.properties = properties == null ? Collections.emptyMap() : properties;
    int port =
        Optional.ofNullable(this.properties.get(PORT))
            .map(Integer::valueOf)
            .orElseThrow(() -> new IllegalArgumentException("Lack required property: " + PORT));

    try {
      this.server = new HTTPServer(port);
    } catch (IOException e) {
      throw new RuntimeException("Start prometheus exporter server failed.", e);
    }
  }

  @Override
  public void close() {
    this.server.close();
  }

  @Override
  public String name() {
    return "prometheus-exporter";
  }

  @Override
  public void setGlobalMetricSet(MetricSet globalMetricSet) {
    boolean skipZeroCounters =
        Optional.ofNullable(properties.get(SKIP_ZERO_COUNTERS))
            .map(Boolean::parseBoolean)
            .orElse(false);
    boolean skipZeroGauges =
        Optional.ofNullable(properties.get(SKIP_ZERO_GAUGES))
            .map(Boolean::parseBoolean)
            .orElse(false);
    MetricsCollector collector =
        new MetricsCollector(globalMetricSet, skipZeroCounters, skipZeroGauges);
    collector.register();
  }
}
