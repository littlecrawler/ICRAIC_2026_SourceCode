# 2026-10-06 dispatch study changes

The revision adds a task-selection mechanism, direct policy comparisons, and
trace-audited workloads tied to healthcare logistics functions. The manuscript
and private response letter are maintained separately from this source repository.

| Concern addressed | Concrete change | Evidence and location |
|---|---|---|
| Technical contribution is limited to implementation | Added `DispatchPolicy`, task-preserving blocking FIFO, and order-preserving `ReadyScan` with one attempt per distinct zone per call. All policies share acquire, recheck, commit and rollback. The original API remains retry-tail by default. | `robot_coordination/src/lib.rs`; manuscript Section III and Fig. 1 |
| No direct baseline comparison | Compare blocking FIFO, original tail retry, and ready scan using the same coordinator and paired input. Blocking FIFO releases the queue before waiting, so it remains a meaningful task-preserving baseline. | `robot_coordination/src/bin/coordination_study.rs`; manuscript Table II and Fig. 2 |
| Workload validation is narrow | Added six scenarios, three worker counts, ten repetitions, seeded paired inputs and shuffled execution order: 540 new runs. | `benchmark-results/revision-20261006/workloads.csv` and `raw_runs.csv`; manuscript Section IV |
| Task counters alone cannot rule out omissions and duplicates | Record every task ID and service interval; runtime checks and an independent CSV audit cover identity, inputs, occupancy, counts and metric reconstruction. | `task_events.csv`, `validation.json`, `scripts/analyze_coordination_study.mjs`; Section IV-B |
| Healthcare framing needs experimental support | Map independent deliveries, a shared pharmacy station, mixed service occupancy, ward-specific batches, request waves, and additional service locations to controlled workload dimensions and metrics. These remain synthetic software abstractions. | Experiment README workload table; manuscript Table I and Sections IV/V |
| Research conclusions need tradeoffs and uncertainty | Report paired bootstrap intervals and all configuration results. Clustered rounds improve; the hotspot does not. Report extra lock attempts and less even allocation in six-worker mixed services. | `summary.csv`, `paired_comparisons.csv`; manuscript Section V |
| Reproduction must include the revision | Add raw observations, full task traces, workload inputs, environment metadata, hash manifest, analysis and verification scripts. Retain the September dataset separately. | `benchmark-results/revision-20261006/` and `scripts/` |

## Evidence recorded in this revision

- 14 automated tests pass: six original integration tests, six new policy tests,
  and two workload/percentile tests.
- 540 timed runs complete all 64,800 expected task executions with zero missing
  or duplicate IDs, zero residual tasks, and no observed zone overlap.
- At three workers, clustered rounds achieve 283.48 tasks/s with ready scan,
  97.70 with blocking FIFO, and 123.69 with tail retry. The mean paired ready-scan
  / blocking-FIFO ratio is 2.90, bootstrap 95% interval [2.85, 2.95].
- The corresponding hotspot ratio is 0.98 [0.96, 0.99], preserving an unfavorable
  result. A heavily shared service station remains the capacity bottleneck.
- In six-worker mixed services, ready scan incurs 31.51 denied attempts/task
  versus 11.32 for tail retry, with count fairness 0.723 versus 0.963.

The method uses established mutex and ready-task selection ideas. It does not
claim a new mutual-exclusion primitive, physical hospital validation, universal
policy superiority, crash recovery, or an acceptance decision.

## Source changes

- `robot_coordination/src/lib.rs`: three policies with a common safe commitment.
- `robot_coordination/tests/dispatch_policies.rs`: policy behavior, exact IDs,
  input rollback, offline eligibility and controlled loss witness.
- `robot_coordination/src/bin/coordination_study.rs`: seeded workloads, paired
  configurations, burst producer, task events, and runtime audit.
- `scripts/analyze_coordination_study.mjs`: independent reconstruction,
  descriptive statistics, bootstrap intervals and manuscript result generation.
- `scripts/verify_snapshot.mjs`: verification of SHA-256 snapshot entries.
- `benchmark-results/revision-20261006/`: measured data and protocol.
- `README.md`: current experiment entry points and repository map.

Original scalability raw files and default demo behavior are preserved.
