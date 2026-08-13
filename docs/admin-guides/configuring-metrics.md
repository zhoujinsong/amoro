---
title: "Configuring Metrics"
url: configuring-metrics
aliases:
    - "admin-guides/configuring-metrics"
menu:
    main:
        parent: Admin Guides
        weight: 500
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -    http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

## Overview

This guide explains how to control which metrics AMS exposes through its `/metrics`
endpoint. AMS publishes four families of per-table / per-group metrics:

* `OptimizerGroupMetrics` (10 metrics, one set per optimizer group);
* `TableOptimizingMetrics` (23 metrics, one set per table);
* `TableSummaryMetrics` (15 metrics, one set per table);
* `TableOrphanFilesCleaningMetrics` (4 metrics, one set per table).

In a deployment with `T` tables and `G` optimizer groups, the legacy default emits
`23·T + 15·T + 4·T + 10·G ≈ 42·T + 10·G` time-series. For deployments with
thousands of tables, this overwhelms most Prometheus / VictoriaMetrics setups.
Starting from this release, AMS supports four mutually exclusive **profiles** that
let operators dial down the volume without modifying source code or dropping series
at the scrape layer.

The rest of this page is organised as follows:

* §0 gives a four-dimension overview of the AMS metric filter stack and points to the
  unified design document.
* §1 introduces the four profiles and their cardinality.
* §2 lists the four configuration keys and their semantics.
* §3 walks through three deployment scenarios.
* §4 explains how to roll back to the legacy behaviour.
* §6/§7/§8 each cover one *value-based* filter that is orthogonal to the
  profile.

## 0. Filter dimensions overview

AMS metric filtering is composed of **four orthogonal, independently togglable
dimensions**. Each dimension has its own configuration key, default value, and
layer of effect. They can be combined freely; the defaults add up to *byte-level
identical behaviour* with the upstream community release.

| # | Dimension | Configuration key | Layer | Default | Section |
|---|---|---|---|:---:|---|
| **D1** | Field whitelist by **profile** | `metric.profile` (+ four `metric.<class>.fields`) | Registration (`MetricRegistry`) | `legacy` | §1, §2 |
| **D2** | Per-value filter on **delete** summary gauges | `metric.summary.skip-zero-deletes` | Registration | `false` | §6 |
| **D3** | Per-value / per-state-threshold filter on **status-class** optimizing gauges | `metric.optimizing.skip-zero-status` + `metric.optimizing.status-duration-threshold-mills.<state>` | Registration | `false`; thresholds default `0` | §7 |
| **D4** | Scrape-side filter on **zero-valued counters** for the Prometheus reporter | `metric.prometheus.skip-zero-counters` (with optional reporter-local override) | Scrape (Prometheus reporter only) | `false` | §8 |

**Key properties of the stack:**

* **Orthogonality.** Whether a metric line ends up in `/metrics` is the AND of
  *(D1 allows the field) ∧ (D2/D3 register decision) ∧ (D4 scrape decision)*.
  Each layer is independent — there is no short-circuit, no implicit coupling.
* **Default safety.** All four switches default to the upstream behaviour
  (`legacy` profile + every value-based switch off). Operators must explicitly
  opt in.
* **Layered effect.** D1/D2/D3 reduce both AMS process memory *and* Prometheus
  storage; D4 only reduces Prometheus scrape payload / TSDB / index size — the
  AMS process still holds every counter in `MetricRegistry`.
* **Reporter scope.** D1/D2/D3 affect every reporter uniformly. D4 is
  intentionally scoped to the Prometheus reporter only, so other reporters
  (OpenTelemetry / StatsD / future plug-ins) are not impacted.
* **Hot-rollback.** Each dimension rolls back independently with a single YAML
  edit and an AMS restart. There is no schema migration and no Thrift / REST
  change.

> **Authoritative reference.** A unified design document combining all four
> dimensions, including the field-level decision tables, mathematical capacity
> projections, and per-task breakdown, is archived at
> `docs/软件设计/2026-05-29-AMS指标体系精简/2026-06-02-AMS指标体系过滤设计.md`. The
> three pre-existing per-dimension design docs (under the same directory) remain
> the authoritative source for the field-level details of D1, D2, and D3+D4.

## 1. Profiles

| Profile       | Total metrics per table / group        | Use case                                                                |
|---------------|----------------------------------------|--------------------------------------------------------------------------|
| `legacy`      | 60 (`23 + 15 + 4` per table, `10` per group) | **Default.** Identical to the pre-trim behaviour. Pick this if your dashboards already depend on the full set. |
| `recommended` | 27 (`6 + 8 + 0` per table, `8` per group)   | New default for green-field deployments. Group U keeps only the 7 raw delete-related signals plus the aggregated `health_score`; capacity attributes (total/data files, sizes, records, snapshots) are dropped because they are obtainable via the AMS REST API and the underlying catalog. Also drops phase-duration sums, per-type counters, and orphan counters. |
| `minimal`     | 19 (`5 + 7 + 0` per table, `5` per group)    | Very large deployments (thousands of tables) where even `recommended` is too heavy. Drops `health_score` on top of `recommended` (Group U keeps only the 7 raw delete signals). |
| `custom`      | Operator-defined                       | Honours four explicit field lists; useful when you need a one-off mix.   |

The exact field membership of each profile is specified in the design document
`docs/软件设计/2026-05-29-AMS指标体系精简/2026-05-29-AMS指标体系精简.md` (§3 and §4).

> **Cross reference.** The metric instances themselves are still constructed
> unconditionally — only the registration step is gated. Therefore switching
> profiles has zero runtime cost on the table refresh path; it only changes what
> the `/metrics` endpoint emits.

## 2. Configuration keys

All four keys live in the AMS top-level configuration (`conf/config.yaml`, under
`ams:`).

### 2.1 `metric.profile`

* **Type:** string, case-insensitive.
* **Allowed values:** `recommended`, `legacy`, `minimal`, `custom`.
* **Default:** `legacy`.
* **Effect:** Selects the profile. When set to `custom`, the four
  `metric.<class>.fields` lists below are honoured verbatim.

### 2.2 `metric.optimizing.fields` *(custom-only)*

* **Type:** list of strings.
* **Default:** empty list (i.e. register nothing under `custom`).
* **Effect:** Whitelist of `TableOptimizingMetrics` field names to register.
  Ignored unless `metric.profile = custom`.

### 2.3 `metric.summary.fields` *(custom-only)*

* **Type:** list of strings.
* **Default:** empty list.
* **Effect:** Whitelist of `TableSummaryMetrics` field names to register. Ignored
  unless `metric.profile = custom`.

### 2.4 `metric.orphan.fields` *(custom-only)*

* **Type:** list of strings.
* **Default:** empty list.
* **Effect:** Whitelist of `TableOrphanFilesCleaningMetrics` field names to
  register. Ignored unless `metric.profile = custom`.

### 2.5 `metric.optimizer-group.fields` *(custom-only)*

* **Type:** list of strings.
* **Default:** empty list.
* **Effect:** Whitelist of `OptimizerGroupMetrics` field names to register.
  Ignored unless `metric.profile = custom`.

### 2.6 Field name reference

The valid field names for each list are the public string constants on the
`org.apache.amoro.server.metrics.MetricFilterConfig` class. The same names are the
suffixes of the metric names emitted by AMS (with the per-class prefix stripped).
For example:

* `pending_tasks`, `executing_tasks`, `pending_tables`, `executing_tables`,
  `committing_tables`, `optimizer_instances`, `memory_bytes_allocated`, `threads`,
  `planing_tables`, `idle_tables` — for `metric.optimizer-group.fields`.
* `lag_duration`, `process_failed_count`, `since_last_optimization`,
  `status_in_pending`, `status_in_executing`, `process_total_count`,
  `status_idle_duration`, ... — for `metric.optimizing.fields`.
* `total_files`, `data_files`, `total_files_size`, `data_files_size`,
  `total_records`, `data_files_records`, `snapshots` (legacy-only — capacity
  attributes, dropped from `recommended`/`minimal`),
  `equality_delete_files`, `position_delete_files`, `dangling_delete_files`,
  `equality_delete_files_size`, `position_delete_files_size`,
  `equality_delete_files_records`, `position_delete_files_records`
  (the 7 raw delete-related health signals registered in `recommended` and
  `minimal`), `health_score` (registered in `recommended` only — dropped from
  `minimal`)
  — for `metric.summary.fields`.
* `orphan_content_file_cleaning_count`, `orphan_metadata_file_cleaning_count`,
  `expected_orphan_content_file_cleaning_count`,
  `expected_orphan_metadata_file_cleaning_count` — for `metric.orphan.fields`.

The complete list is enumerated in the design document §2.

## 3. Worked examples

### 3.1 Switching an existing cluster to the recommended profile

Before performing the switch, audit your dashboards / alerting rules: any
expression that references one of the dropped fields will become a no-op. The
design document §3.4 enumerates the dropped fields together with their migration
hints.

```yaml
ams:
  # ...
  metric:
    profile: recommended
```

After AMS restart, `/metrics` will expose ~55 % fewer series (60 → 27 per
table / group), which translates to roughly the same percentage drop in scrape
payload size and Prometheus storage. The `recommended` profile is built around
the operational principle that **AMS metrics monitor table health, not table
capacity** — so dashboards that read `total_files` / `total_files_size` /
`total_records` / `snapshots` from Prometheus need to either (a) pin to
`legacy`, (b) re-add those fields under a `custom` profile, or (c) migrate the
panel to the AMS REST API. The underlying values are unchanged — only the
Prometheus publication is gated.

### 3.2 Tightening further with the minimal profile

Use `minimal` only after you have already moved to `recommended` and confirmed
that the alerting set is sufficient for your operational needs.

```yaml
ams:
  metric:
    profile: minimal
```

`minimal` registers no orphan-file metrics, no `health_score`, and no capacity
attributes. If your runbooks rely on `orphan_content_file_cleaning_count` or on
the aggregated `health_score`, prefer `recommended`. The 7 raw delete-related
signals (`equality_delete_files`, `position_delete_files`,
`dangling_delete_files`, and their `*_size` / `*_records` companions) are still
registered, and they are sufficient to drive read-amplification alarms.

### 3.3 A custom mix tuned to a specific dashboard

Suppose you only need `lag_duration`, `process_failed_count`, the four
`*_total_files*` summary fields, and the eight optimizer-group fields:

```yaml
ams:
  metric:
    profile: custom
    optimizing:
      fields:
        - lag_duration
        - process_failed_count
    summary:
      fields:
        - total_files
        - data_files
        - total_files_size
        - data_files_size
    orphan:
      fields: []
    optimizer-group:
      fields:
        - pending_tasks
        - executing_tasks
        - pending_tables
        - executing_tables
        - committing_tables
        - optimizer_instances
        - memory_bytes_allocated
        - threads
```

Unknown field names are silently ignored (they are simply never matched), so a
typo in this list is a soft failure: the corresponding metric stays
unregistered. Verify with `curl http://<ams-host>:1630/metrics | grep -c
'<metric_name>'` after restart.

## 4. Rollback

Switching back to the previous behaviour is a one-line change:

```yaml
ams:
  metric:
    profile: legacy
```

`legacy` is also the default value when the key is absent, so simply removing the
`metric.profile` line achieves the same effect.

## 5. Observability of the filter itself

On startup AMS logs the active profile at `INFO` level under the
`org.apache.amoro.server.metrics.MetricFilterConfig` logger. Search for
`profile=` in `ams.log` to confirm what is in effect:

```
2026-05-29 17:00:00 INFO  o.a.a.s.metrics.MetricFilterConfig - Metric filter installed: profile=RECOMMENDED ...
```

If the active profile is unexpectedly `LEGACY`, double-check that the
configuration file is the one AMS actually loaded (the path is logged earlier in
the same startup line) and that the YAML key indentation is correct.

## 6. Value-based filter for delete-related summary metrics

Independently of `metric.profile`, AMS exposes a global switch that can further
trim the seven *delete-related* `TableSummaryMetrics` fields by their **current
value**. When the switch is on, a delete metric is registered with the
`MetricRegistry` only while its value is greater than zero; as soon as the value
returns to zero the metric is dynamically unregistered, removing the time series
from `/metrics`.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `metric.summary.skip-zero-deletes` | boolean | `false` | When `true`, the seven delete-related summary fields are only registered while their value is > 0. Each metric is judged independently against its own value (no joint condition). When `false` (default) the upstream behaviour of always registering is preserved. |

The seven managed fields are:

```
table_summary_equality_delete_files
table_summary_position_delete_files
table_summary_dangling_delete_files
table_summary_equality_delete_files_size
table_summary_position_delete_files_size
table_summary_equality_delete_files_records
table_summary_position_delete_files_records
```

`table_summary_health_score` is **not** affected by this switch — it is an
aggregated signal whose semantics include "100 means healthy", so it must remain
registered even when its value is non-positive.

### 6.1 Relationship with `metric.profile`

The switch and the profile are **orthogonal**: the profile decides whether a
field is *allowed* at all; the switch then decides *how* the allowed delete
fields are registered (eagerly vs. by value). A field that is rejected by the
profile is never registered regardless of the switch.

| profile | switch | effect on the seven delete fields |
|---|---|---|
| any | `false` (default) | all seven (subject to profile whitelist) registered eagerly — same as upstream |
| `recommended` / `minimal` / `legacy` / `custom` | `true` | each of the seven (subject to profile whitelist) is registered iff its current value > 0; transitions on every refresh |

### 6.2 Worked example

```yaml
ams:
  metric:
    profile: recommended
    summary:
      skip-zero-deletes: true
```

For an Iceberg non-keyed table that never receives equality / position deletes,
the seven delete time series **never appear** in `/metrics`, saving roughly
seven series per such table from the Prometheus storage. The moment a delete
file shows up after a write, the matching metric is dynamically registered on
the next AMS refresh cycle (typically minute-level cadence).

### 6.3 Operational impact and rollback

* **Series appear / disappear over time.** Dashboards that render the seven
  fields as table rows will show rows blinking in and out of existence — this is
  the expected outcome and matches the "healthy table → no signal" semantics.
  PromQL `rate()` / `increase()` are inapplicable to these gauges in the first
  place, so no historical aggregation is broken.
* **Alerts based on `> threshold`** (e.g. `equality_delete_files > 100`) fire
  the same way as before, because the series only disappears when the value is
  zero — well below any real threshold.
* **Rollback is one line:** set `metric.summary.skip-zero-deletes: false` (or
  remove the key) and restart AMS. The previous always-registered behaviour is
  restored without any data migration.

On startup AMS prints the effective switch value alongside the profile:

```
2026-06-01 17:00:00 INFO  o.a.a.s.metrics.MetricFilterConfig - Metric filter installed: profile=RECOMMENDED optimizing=6 summary=8 orphan=0 group=8 skipZeroDeletes=true
```

## 7. Value/threshold-based filter for status-class optimizing metrics

Independently of `metric.profile` and `metric.summary.skip-zero-deletes`, AMS
exposes a second global switch that trims the **10 status-class
`TableOptimizingMetrics` fields** by their **current value** against an
(optional) per-state threshold. When the switch is on, each status-class metric
is registered with the `MetricRegistry` only while its value strictly exceeds
the effective threshold; as soon as it falls back the metric is dynamically
unregistered, removing the time series from `/metrics`.

### 7.1 Configuration keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `metric.optimizing.skip-zero-status` | boolean | `false` | Master switch. When `false` (default) the upstream behaviour of always registering is preserved. When `true` the 10 status-class fields enter the dynamic register/unregister loop. |
| `metric.optimizing.status-duration-threshold-mills.idle` | long | `0` | Independent threshold (ms) for `status_idle_duration_mills`. Negative values are coerced to `0` with a `WARN` log. |
| `metric.optimizing.status-duration-threshold-mills.pending` | long | `0` | Independent threshold (ms) for `status_pending_duration_mills`. |
| `metric.optimizing.status-duration-threshold-mills.planning` | long | `0` | Independent threshold (ms) for `status_planning_duration_mills`. |
| `metric.optimizing.status-duration-threshold-mills.executing` | long | `0` | Independent threshold (ms) for `status_executing_duration_mills`. |
| `metric.optimizing.status-duration-threshold-mills.committing` | long | `0` | Independent threshold (ms) for `status_committing_duration_mills`. |

The 10 managed fields are:

```
amoro_table_optimizing_status_in_idle
amoro_table_optimizing_status_in_pending
amoro_table_optimizing_status_in_planning
amoro_table_optimizing_status_in_executing
amoro_table_optimizing_status_in_committing
amoro_table_optimizing_status_idle_duration_mills
amoro_table_optimizing_status_pending_duration_mills
amoro_table_optimizing_status_planning_duration_mills
amoro_table_optimizing_status_executing_duration_mills
amoro_table_optimizing_status_committing_duration_mills
```

The remaining 13 `TableOptimizingMetrics` fields (8 process counters, 4
`since_last_*_optimization` gauges, `lag_duration`) are **not** affected by this
switch — they preserve the upstream eager-registration behaviour.

### 7.2 How it works

For each of the 10 fields, two judgements run on every state change and on
every periodic table refresh:

| Field family | Threshold source | Register when |
|---|---|---|
| `status_in_*` | implicit `0` | `value > 0` (i.e. the table is currently in that state) |
| `status_*_duration_mills` | per-state independent threshold (default `0`) | `value > effectiveThreshold(state)` |

The judgements are **per-metric**: each of the 10 fields is decided
independently. The mathematical equivalence `status_in_X == 1` ⇔
`status_X_duration_mills > 0` guarantees that at any instant **at most two
status-class series** per table are exposed: one `status_in_X = 1` for the
current state, and (optionally) one `status_X_duration_mills` if the table is
stuck in that state long enough to cross the threshold.

### 7.3 Relationship with `metric.profile`

The switch and the profile are **orthogonal**: the profile decides whether a
field is *allowed* at all (e.g. `recommended` blacklists `status_in_idle` and
all 5 duration fields); the switch then decides *how* the allowed status-class
fields are registered (eagerly vs. by value/threshold). A field that is
rejected by the profile is never registered regardless of the switch.

### 7.4 Recommended production configuration

For a 10 000-table cluster the following configuration cuts the status-class
series count from `10·T` (LEGACY) or `2·T` (RECOMMENDED) down to `≈ T` —
typically one `status_in_idle = 1` per healthy table plus a handful of
`*_duration_mills` series for stuck tables:

```yaml
ams:
  metric:
    profile: recommended
    summary:
      skip-zero-deletes: true
    optimizing:
      skip-zero-status: true
      status-duration-threshold-mills:
        # IDLE keeps 0 — any positive idle_duration is exposed (community semantics).
        pending:    1800000   # 30 min
        planning:    300000   # 5 min
        executing: 3600000    # 1 hour
        committing:  600000   # 10 min
```

Tune each threshold to match the SLA your alerting rules already encode. The
five thresholds are independent on purpose: their realistic values differ by
two orders of magnitude (PLANNING is typically sub-second, EXECUTING can run
for an hour), and a single global value would either be too noisy for fast
states or hide real incidents in slow states.

### 7.5 Operational impact and rollback

* **Series appear / disappear over time.** Same blink-in/out semantics as
  `skip-zero-deletes`. PromQL `rate()` / `increase()` are inapplicable to
  these gauges anyway, so no historical aggregation is broken.
* **Threshold-crossing latency.** A `status_*_duration_mills` series appears
  on the next AMS table-refresh cycle (typically minute-level cadence) after
  the threshold is crossed. Combined with the Prometheus scrape interval
  (~15 s), the worst-case detection lag is about one refresh cycle. For a
  30-minute PENDING threshold this means the first alert arrives at ~31 min,
  which is acceptable for capacity-class signals.
* **Alerts based on `> threshold` continue to work.** PromQL such as
  `amoro_table_optimizing_status_pending_duration_mills > 1800000` still fires
  the same way — the series only disappears when its value is below the
  configured registration threshold, well below any operational alert
  threshold.
* **`count(metric)` semantics shift.** Queries that count series by their
  existence (e.g. `count(amoro_table_optimizing_status_in_pending)`) will see
  fewer series after enabling the switch. Migrate such queries to the
  semantically explicit form `count(amoro_table_optimizing_status_in_pending == 1)`.
* **Rollback is one line:** set `metric.optimizing.skip-zero-status: false`
  (or remove the key) and restart AMS. The previous always-registered behaviour
  is restored without any data migration. **No code rollback is needed.**

On startup AMS prints the effective thresholds alongside the profile and the
two switches:

```
2026-06-01 17:00:00 INFO  o.a.a.s.metrics.MetricFilterConfig - Metric filter installed: profile=RECOMMENDED optimizing=6 summary=8 orphan=0 group=8 skipZeroDeletes=true skipZeroStatus=true statusThresholdByState={idle=0, pending=1800000, planing=300000, executing=3600000, committing=600000}
```

## 8. Scrape-side filter for zero-valued Prometheus counters

Independently of `metric.profile`, `metric.summary.skip-zero-deletes`, and
`metric.optimizing.skip-zero-status`, AMS exposes a fourth global switch that
operates on the **Prometheus reporter scrape side** rather than the
`MetricRegistry` registration side. When enabled, the Prometheus reporter omits
any `Counter` sample whose current cumulative value equals `0` from the
`/metrics` text response. The `MetricRegistry` is untouched: the counter
instance still lives in AMS memory, and any other reporter (e.g. OpenTelemetry)
still sees it.

### 8.1 Configuration keys

| Key | Layer | Type | Default | Meaning |
|---|---|---|:---:|---|
| `metric.prometheus.skip-zero-counters` | global (`conf/config.yaml`) | boolean | `false` | When `true`, the Prometheus reporter skips `Counter` samples whose current value equals `0` in `/metrics` output. Has no effect on `Gauge` samples and no effect on other reporters. |
| `skip-zero-counters` | reporter-local (`plugins/metric-reporters.yaml`, under the `prometheus-exporter` reporter's `properties:`) | boolean | inherits the global value | Local override. Explicit `true` / `false` here wins over the global value. |

**Precedence** (matching mainstream `putIfAbsent` semantics):

```
reporter.properties.skip-zero-counters  >  metric.prometheus.skip-zero-counters  >  default false
```

The managed-fields scope is the **8 Counter-typed fields** of
`TableOptimizingMetrics` (`process_total_count`, `process_failed_count`, plus
the 6 per-type minor / major / full total / failed counters) — and any future
field AMS registers as `Counter`. **Gauges are never affected** by this
switch, regardless of their value, because Gauge zero often carries an explicit
signal (e.g. `health_score = 0`).

### 8.2 Why scrape-side and not registration-side

Counter values are **cumulative**. A `unregister → register` round-trip would
reset the count to zero and break Prometheus `rate()` / `increase()`
calculations across the gap. By filtering at the scrape stage we keep the
counter accumulating in AMS memory; the next scrape after the value crosses
`0 → > 0` automatically restores the series, and Prometheus's existing
first-sample handling treats it like any other counter resumption.

The trade-off:

* **No AMS memory savings** — the `MetricRegistry` still holds every counter.
* **Significant Prometheus savings** — for typical 100k-table deployments where
  the vast majority of tables never experienced a failure or never ran an
  optimization of a given type, 95 %+ of counter rows are eliminated from each
  scrape, with the same percentage drop in TSDB storage and Grafana index size.

### 8.3 Relationship with the other three dimensions

This switch is **orthogonal to D1/D2/D3**. A counter line appears in `/metrics`
if and only if all of the following hold:

1. The profile (D1) allows the field. Otherwise the field is never registered
   in the first place — D4 has nothing to filter.
2. The current cumulative value of the counter, when read at scrape time, is
   `> 0`, **or** D4 is off (`metric.prometheus.skip-zero-counters: false` and
   no reporter-local override).

The switch never affects Gauges. Therefore D2 (delete gauges) and D3 (status
gauges) operate on completely disjoint metric sets from D4 — there is no way
for the four switches to interfere with each other, even when all four are
enabled simultaneously.

### 8.4 Worked example

A typical large-cluster recommended configuration enabling all four dimensions:

```yaml
ams:
  metric:
    profile: recommended
    summary:
      skip-zero-deletes: true
    optimizing:
      skip-zero-status: true
      status-duration-threshold-mills:
        pending:    1800000
        planning:    300000
        executing: 3600000
        committing:  600000
    prometheus:
      skip-zero-counters: true
```

If you operate **multiple** Prometheus reporter instances and want one of them
to retain zero-valued counter rows for debugging:

```yaml
# plugins/metric-reporters.yaml
metric-reporters:
  - name: prometheus-exporter
    enabled: true
    properties:
      port: 7001
      # No skip-zero-counters here → inherits the global true above.
  - name: prometheus-exporter
    enabled: true
    properties:
      port: 7002
      skip-zero-counters: false   # Local override: this instance keeps zeros.
```

### 8.5 Migration guidance

* **`absent()`-style alerts must be rewritten.** Queries such as
  `absent(amoro_table_optimizing_process_failed_count_total{table="foo"})` were
  previously a stable way to detect *the table has never reported a failure*.
  Once D4 is on, the same series can disappear simply because the cumulative
  value is still `0`. Migrate such alerts to value-aware forms, for example
  `... > 0` or `rate(...) > 0`.
* **`rate()` / `increase()` are unaffected** for series that have ever been
  non-zero. Prometheus's own counter-reset handling treats the
  zero → first-non-zero transition like a normal increment.
* **`count(metric)`-style queries** that count the number of distinct counter
  series will see fewer series after enabling D4. Prefer
  `count(metric > 0)` or migrate the panel to AMS REST APIs when the
  *existence* of a series is what you are measuring.

### 8.6 Rollback

Setting `metric.prometheus.skip-zero-counters: false` (or removing the key, and
deleting any reporter-local override) and restarting AMS restores the upstream
behaviour. There is no data migration. Other reporters were never affected, so
they require no rollback step.
