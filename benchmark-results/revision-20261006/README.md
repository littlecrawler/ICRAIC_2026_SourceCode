# Resource-aware dispatch comparison

This dataset was measured on 2026-10-06. It contains **540 new runs and 64,800
task executions**, separate from the 2026-09-15 scalability measurements in the
parent directory. No row from the old experiment is relabeled as a new run.

## Policies and common implementation

- `fifo_blocking` (B): remove the queue head, release the queue lock, then wait
  for its zone. The worker retains the task while waiting. Other workers can
  continue taking tasks; this is not a queue-wide blocking baseline.
- `retry_tail` (R): the original dispatch policy. Try the head task, append it
  to the tail when its zone is occupied, and sleep 1 ms before another call.
- `ready_scan` (S): scan in queue order, trying each distinct zone at most once
  per call. Remove the first task whose zone can be locked, leaving skipped
  tasks in order. If all candidate zones are busy, retain the queue and sleep
  1 ms. Its worst-case selection cost is O(Q + m), including bitmap creation
  and deque removal, versus O(1) head selection for B/R.

All policies use the same coordinator, zone mutexes, eligibility recheck,
completion code, counters, heartbeat calls, and event instrumentation. They
acquire the zone before committing busy state and restore the task if the
post-acquisition eligibility check fails. The existing default API still
selects `RetryTail`. A worker owns at most one task/zone. No background
heartbeat monitor is active in timed runs. Stale-worker behavior is tested
separately. The experiment does not introduce a robot driver or navigation stack.

## Workloads and service functions

All configurations have 120 task IDs and use 1, 3, or 6 workers. Ten repetitions
give 6 workloads x 3 policies x 3 worker counts x 10 = 540 measured runs.

| Workload | Exact controlled input | Motivating service-robot function | Main question |
|---|---|---|---|
| `ward_delivery` | Three zones; 40 tasks per zone; 10 ms; seeded shuffle | Delivery to independent handover locations | Parallel dispatch and response time |
| `pharmacy_hotspot` | Zone shares 96/12/12 tasks; 10 ms; seeded shuffle | Many deliveries sharing a pickup station | Shared-resource capacity |
| `mixed_services` | Three zones; per zone 14 x 5 ms, 13 x 10 ms, 13 x 40 ms; seeded shuffle | Short handovers and longer service-location occupancy | Sensitivity to service duration |
| `clustered_rounds` | 40 zone-0 tasks, then 40 zone-1 tasks, then 40 zone-2 tasks; 10 ms | Orders entered as ward-specific batches | Bypass of queued unavailable work |
| `burst_requests` | Three zones; shuffled tasks; batches of 40 at 0/200/400 ms; 10 ms | Requests arriving in waves | Arrival-to-completion response |
| `multi_ward` | Six zones; 20 tasks per zone; 10 ms; seeded shuffle | More independent handover locations | Scaling available resources |

These are synthetic analogies, not hospital logs. Durations accelerate the
software experiment and are not clinical, travel, or disinfection times. Zone
exclusion represents a reservation; it is not a collision or human-safety metric.

Base seed: `20261006`. Input seed = base + 100 x repetition + scenario index
(scenario order is listed above, index starts at zero). `StdRng` uses the pinned
`rand` dependency. Each policy and worker count uses the same input within a
repetition. Configuration order is shuffled within each repetition with a
separate seeded generator. `workloads.csv` makes the actual inputs auditable
without assuming a future PRNG implementation is unchanged. Clustered rounds
have a deterministic task order; repetitions capture scheduling variation there.

## Environment and timing

Windows x86-64, Intel Core i9-14900HX, 32 available logical processors,
Rust 1.94.1, Cargo release optimization. `environment.txt` records the runtime
platform, processor identifier, timestamp, compiler, and parameters. A single
workstation was used. Machine load and operating-system timing were not isolated.

Workers synchronize at a barrier. The epoch is set immediately before the main
thread enters that barrier; task generation, initial enqueue, and thread creation
are excluded. Makespan ends at the latest task completion, excluding thread joins
and CSV output. Initial tasks have arrival time zero. The producer records the
actual enqueue timestamp just before inserting each later batch; response time
uses that timestamp rather than the nominal 200/400-ms deadline. OS timer
resolution, scheduling, and sleep overshoot remain part of the measurement.

## Metrics and independent checks

- Throughput: completed tasks / makespan in seconds.
- Response p95: nearest-rank 95th percentile of completion minus actual enqueue
  time, computed within each run. Wait p95 uses service start minus enqueue.
- Denials: failed nonblocking zone acquisitions. B waits inside a mutex, so its
  zero denials must not be interpreted as zero contention. A scan can attempt
  multiple busy zones in one call. These counts do not measure CPU or energy.
- Jain fairness: square of total completions divided by worker count times the
  sum of squared worker completion counts. This measures count allocation,
  not duration-weighted effort, priorities, or deadlines.
- Each run asserts expected task IDs exactly once, matching completion counters,
  no queue residue, atomic zone occupancy at most one, and disjoint recorded
  service intervals. The analysis script independently reconstructs latency,
  makespan, throughput, and fairness from `task_events.csv`, verifies inputs
  against `workloads.csv`, and checks IDs and zone intervals.

`summary.csv` reports means and sample SD. `paired_comparisons.csv` compares S
with B and R, pairing equal workload/repetition/worker count and averaging
per-run throughput ratios. It uses 10,000 paired bootstrap resamples with a
fixed analysis seed and percentile 95% intervals. Ten repetitions are a small
sample; intervals are descriptive for this host, not simultaneous significance
tests or replication across machines. All 54 configuration summaries and all
36 ratio comparisons are retained, including unfavorable results.

## Measured findings

| Three-worker workload | B throughput | R throughput | S throughput | Mean paired S/B ratio and 95% interval |
|---|---:|---:|---:|---|
| Ward delivery | 190.56 | 216.62 | 284.38 | 1.49 [1.47, 1.52] |
| Pharmacy hotspot | 119.24 | 111.01 | 116.52 | 0.98 [0.96, 0.99] |
| Mixed services | 98.69 | 132.96 | 158.70 | 1.61 [1.54, 1.68] |
| Clustered rounds | 97.70 | 123.69 | 283.48 | 2.90 [2.85, 2.95] |
| Burst requests | 196.23 | 200.26 | 209.38 | 1.07 [1.04, 1.09] |
| Multi-ward | 233.97 | 257.22 | 278.10 | 1.19 [1.16, 1.22] |

Throughput units are tasks/s. S is slower than B at the hotspot. In six-worker
mixed services, S has 31.51 denied attempts/task versus 11.32 for R, and count
fairness is 0.723 versus 0.963. These tradeoffs are part of the result.
The source-level lower bound on makespan is the maximum of total requested
service / worker count, per-zone requested service, and latest release + service.
Thus the 80% hotspot limits ideal throughput to 125 tasks/s even with extra
workers; three and six balanced zones permit 300 and 600 tasks/s respectively.

## Reproduce

From the repository root, with Rust and Node.js 22 or later installed:

```powershell
cargo test --locked --manifest-path robot_coordination/Cargo.toml --all-targets
cargo run --locked --release --manifest-path robot_coordination/Cargo.toml --bin coordination_study -- --output benchmark-results/local/my-study
node scripts/analyze_coordination_study.mjs benchmark-results/local/my-study
node scripts/verify_snapshot.mjs
```

Use a fresh output directory: the runner refuses to replace existing raw files.
Defaults reproduce this protocol. `--repetitions`, `--seed`, and `--tasks` are
available for exploratory runs; tasks must be positive and divisible by 30.
The local output directory is ignored by Git. Analyze the committed dataset with:

```powershell
node scripts/analyze_coordination_study.mjs benchmark-results/revision-20261006
```

The analysis writes deterministic summaries and `validation.json`, leaving raw
observations and task traces unchanged. An optional second path argument emits
the local manuscript's numerical macros, comparison table, and scaling plot;
manuscript files are not part of this source repository.

## Files

`raw_runs.csv`: 540 runs; `task_events.csv`: 64,800 executed tasks;
`workloads.csv`: 7,200 input tasks (six scenarios x ten repetitions x 120 tasks);
`summary.csv`: 54 groups; `paired_comparisons.csv`: 36 comparisons;
`validation.json`: independent audit; `environment.txt`: execution metadata;
`SHA256SUMS.txt`: hashes of the evaluated source, documentation, and observations.

Failure recovery, network delays, motion, clinical outcomes, and multi-host
validation remain outside this experiment. The deliberately faulty drop case
is a deterministic test witness, not an optimized performance baseline.
