//! Paired, seeded policy comparison with task-level accounting and occupancy checks.
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use robot_coordination::{DispatchOutcome, DispatchPolicy, Robot, RobotCoordinator, Task};
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCENARIOS: [&str; 6] = [
    "ward_delivery",
    "pharmacy_hotspot",
    "mixed_services",
    "clustered_rounds",
    "burst_requests",
    "multi_ward",
];
const POLICIES: [(&str, DispatchPolicy); 3] = [
    ("fifo_blocking", DispatchPolicy::FifoBlocking),
    ("retry_tail", DispatchPolicy::RetryTail),
    ("ready_scan", DispatchPolicy::ReadyScan),
];

struct Config {
    repetitions: u32,
    tasks: u32,
    seed: u64,
    output: PathBuf,
}
#[derive(Clone)]
struct Scheduled {
    task: Task,
    release_ms: u64,
}
struct Event {
    task_id: u32,
    zone: u32,
    duration_ms: u64,
    worker: u32,
    arrival_ms: f64,
    start_ms: f64,
    end_ms: f64,
    finish_ms: f64,
}
struct Run {
    elapsed_ms: f64,
    throughput: f64,
    p95_response_ms: f64,
    p95_wait_ms: f64,
    denials: u32,
    attempts: u32,
    fairness: f64,
    remaining: usize,
    completed: u32,
    missing: usize,
    duplicates: usize,
    overlaps: usize,
    events: Vec<Event>,
}

fn workload(name: &str, count: u32, seed: u64) -> (u32, Vec<Scheduled>) {
    let zones = if name == "multi_ward" { 6 } else { 3 };
    let mut rng = StdRng::seed_from_u64(seed);
    let mut specifications: Vec<(u32, u64)> = (0..count)
        .map(|i| {
            let zone = if name == "pharmacy_hotspot" {
                if i < count * 8 / 10 {
                    0
                } else if i < count * 9 / 10 {
                    1
                } else {
                    2
                }
            } else if name == "clustered_rounds" {
                i / (count / 3)
            } else {
                i % zones
            };
            // Duration varies independently of zone; each zone sees all three sizes.
            let duration = if name == "mixed_services" {
                [5, 10, 40][(i / zones % 3) as usize]
            } else {
                10
            };
            (zone, duration)
        })
        .collect();
    if name != "clustered_rounds" {
        specifications.shuffle(&mut rng);
    }
    let tasks = specifications
        .into_iter()
        .enumerate()
        .map(|(i, (zone, duration))| Scheduled {
            task: Task::new(i as u32, zone, duration, name),
            release_ms: if name == "burst_requests" {
                (i as u64 / (u64::from(count) / 3)) * 200
            } else {
                0
            },
        })
        .collect();
    (zones, tasks)
}

fn p95(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[(values.len() * 95).div_ceil(100) - 1]
}

fn run_once(scheduled: &[Scheduled], zones: u32, workers: u32, policy: DispatchPolicy) -> Run {
    let coordinator = Arc::new(RobotCoordinator::new(zones));
    let arrivals = Arc::new(
        (0..scheduled.len())
            .map(|_| OnceLock::<f64>::new())
            .collect::<Vec<_>>(),
    );
    let occupancy = Arc::new((0..zones).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let violations = Arc::new(AtomicUsize::new(0));
    let producer_done = Arc::new(AtomicBool::new(false));
    let epoch = Arc::new(OnceLock::<Instant>::new());
    let barrier = Arc::new(Barrier::new(workers as usize + 2));
    let initial: Vec<Task> = scheduled
        .iter()
        .filter(|t| t.release_ms == 0)
        .map(|t| {
            arrivals[t.task.id as usize].set(0.0).unwrap();
            t.task.clone()
        })
        .collect();
    coordinator.add_tasks(initial);
    for worker in 0..workers {
        coordinator
            .register_robot(Robot::new(worker, &format!("Worker-{worker}")))
            .unwrap();
    }
    let producer = {
        let c = Arc::clone(&coordinator);
        let a = Arc::clone(&arrivals);
        let e = Arc::clone(&epoch);
        let b = Arc::clone(&barrier);
        let done = Arc::clone(&producer_done);
        let remaining: Vec<Scheduled> = scheduled
            .iter()
            .filter(|t| t.release_ms > 0)
            .cloned()
            .collect();
        thread::spawn(move || {
            b.wait();
            let start = *e.get().unwrap();
            let mut cursor = 0;
            while cursor < remaining.len() {
                let release = remaining[cursor].release_ms;
                let target = start + Duration::from_millis(release);
                if let Some(wait) = target.checked_duration_since(Instant::now()) {
                    thread::sleep(wait);
                }
                let actual = start.elapsed().as_secs_f64() * 1000.0;
                let mut batch = Vec::new();
                while cursor < remaining.len() && remaining[cursor].release_ms == release {
                    let task = remaining[cursor].task.clone();
                    a[task.id as usize].set(actual).unwrap();
                    batch.push(task);
                    cursor += 1;
                }
                c.add_tasks(batch);
            }
            done.store(true, Ordering::Release);
        })
    };
    let mut handles = Vec::new();
    for worker in 0..workers {
        let c = Arc::clone(&coordinator);
        let a = Arc::clone(&arrivals);
        let e = Arc::clone(&epoch);
        let b = Arc::clone(&barrier);
        let done = Arc::clone(&producer_done);
        let occupied = Arc::clone(&occupancy);
        let overlap = Arc::clone(&violations);
        handles.push(thread::spawn(move || {
            let mut events = Vec::new();
            b.wait();
            let start = *e.get().unwrap();
            loop {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "run exceeded 30 seconds"
                );
                c.heartbeat(worker);
                match c.dispatch_next_task_with_policy(worker, policy).unwrap() {
                    DispatchOutcome::Assigned { task, zone_guard } => {
                        let zone = task.zone as usize;
                        if occupied[zone].fetch_add(1, Ordering::SeqCst) != 0 {
                            overlap.fetch_add(1, Ordering::SeqCst);
                        }
                        let start_ms = start.elapsed().as_secs_f64() * 1000.0;
                        thread::sleep(Duration::from_millis(task.duration_ms));
                        let end_ms = start.elapsed().as_secs_f64() * 1000.0;
                        assert_eq!(occupied[zone].fetch_sub(1, Ordering::SeqCst), 1);
                        assert!(c.leave_zone(zone_guard, worker, zone));
                        c.complete_task(worker).unwrap();
                        events.push(Event {
                            task_id: task.id,
                            zone: task.zone,
                            duration_ms: task.duration_ms,
                            worker,
                            arrival_ms: *a[task.id as usize].get().unwrap(),
                            start_ms,
                            end_ms,
                            finish_ms: start.elapsed().as_secs_f64() * 1000.0,
                        });
                    }
                    DispatchOutcome::ZoneBusy { .. } => thread::sleep(Duration::from_millis(1)),
                    DispatchOutcome::QueueEmpty => {
                        // Recheck after acquiring the producer flag: an enqueue can
                        // race with the first empty observation.
                        if done.load(Ordering::Acquire) && c.queue_length() == 0 {
                            break;
                        }
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
            events
        }));
    }
    epoch.set(Instant::now()).unwrap();
    barrier.wait();
    let mut events = Vec::new();
    let mut counts = Vec::new();
    for handle in handles {
        let local = handle.join().expect("worker panicked");
        counts.push(local.len() as f64);
        events.extend(local);
    }
    producer.join().unwrap();
    let stats = coordinator.get_stats();
    let elapsed_ms = events.iter().map(|e| e.finish_ms).fold(0.0, f64::max);
    let mut occurrences = vec![0usize; scheduled.len()];
    for event in &events {
        occurrences[event.task_id as usize] += 1;
    }
    let missing = occurrences.iter().filter(|&&n| n == 0).count();
    let duplicates = occurrences.iter().map(|n| n.saturating_sub(1)).sum();
    let overlaps = violations.load(Ordering::SeqCst);
    assert_eq!(
        (missing, duplicates, overlaps, coordinator.queue_length()),
        (0, 0, 0, 0)
    );
    assert_eq!(stats.total_tasks_completed as usize, scheduled.len());
    assert_eq!(stats.total_zone_access_grants as usize, scheduled.len());
    for zone in 0..zones {
        let mut intervals: Vec<&Event> = events.iter().filter(|e| e.zone == zone).collect();
        intervals.sort_by(|a, b| a.start_ms.total_cmp(&b.start_ms));
        assert!(
            intervals
                .windows(2)
                .all(|pair| pair[0].end_ms <= pair[1].start_ms)
        );
    }
    let total = counts.iter().sum::<f64>();
    Run {
        elapsed_ms,
        throughput: events.len() as f64 * 1000.0 / elapsed_ms,
        p95_response_ms: p95(events.iter().map(|e| e.finish_ms - e.arrival_ms).collect()),
        p95_wait_ms: p95(events.iter().map(|e| e.start_ms - e.arrival_ms).collect()),
        denials: stats.total_zone_denials,
        attempts: stats.total_zone_access_attempts,
        fairness: total * total / (f64::from(workers) * counts.iter().map(|n| n * n).sum::<f64>()),
        remaining: coordinator.queue_length(),
        completed: stats.total_tasks_completed,
        missing,
        duplicates,
        overlaps,
        events,
    }
}

fn config() -> Result<Config, String> {
    let mut c = Config {
        repetitions: 10,
        tasks: 120,
        seed: 20261006,
        output: "../benchmark-results/local/coordination-study".into(),
    };
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!(
                "coordination_study [--repetitions N] [--tasks N] [--seed N] [--output DIR]\nTasks must be a positive multiple of 30.\nDefaults: 10 repetitions, 120 tasks, seed 20261006; 6 scenarios x 3 policies x 3 worker counts."
            );
            std::process::exit(0);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {arg}"))?;
        match arg.as_str() {
            "--repetitions" => c.repetitions = value.parse().map_err(|_| "invalid repetitions")?,
            "--tasks" => c.tasks = value.parse().map_err(|_| "invalid tasks")?,
            "--seed" => c.seed = value.parse().map_err(|_| "invalid seed")?,
            "--output" => c.output = value.into(),
            _ => return Err(format!("unknown option: {arg}")),
        }
    }
    if c.repetitions == 0 || c.tasks == 0 || !c.tasks.is_multiple_of(30) {
        return Err("positive repetitions and task count divisible by 30 required".into());
    }
    Ok(c)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let c = config()?;
    // A reference run is immutable: use a fresh output directory for each run.
    fs::create_dir_all(&c.output)?;
    let mut raw = BufWriter::new(File::create_new(c.output.join("raw_runs.csv"))?);
    let mut traces = BufWriter::new(File::create_new(c.output.join("task_events.csv"))?);
    let mut inputs = BufWriter::new(File::create_new(c.output.join("workloads.csv"))?);
    let mut environment = File::create_new(c.output.join("environment.txt"))?;
    writeln!(
        environment,
        "unix_start_seconds={}\nos={}\narch={}\nlogical_processors={}\nprocessor={}\nrepetitions={}\ntasks={}\nseed={}\nretry_sleep_ms=1\nprofile=release",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        env::consts::OS,
        env::consts::ARCH,
        thread::available_parallelism()?.get(),
        env::var("PROCESSOR_IDENTIFIER").unwrap_or_default(),
        c.repetitions,
        c.tasks,
        c.seed
    )?;
    if let Ok(version) = std::process::Command::new("rustc")
        .arg("--version")
        .output()
    {
        writeln!(
            environment,
            "{}",
            String::from_utf8_lossy(&version.stdout).trim()
        )?;
    }
    writeln!(
        raw,
        "order,scenario,policy,workers,repetition,seed,tasks,zones,elapsed_ms,throughput_tasks_s,p95_response_ms,p95_wait_ms,zone_denials,zone_attempts,jain_fairness,completed,remaining,missing,duplicates,overlaps"
    )?;
    writeln!(
        traces,
        "order,scenario,policy,workers,repetition,task_id,zone,duration_ms,worker,arrival_ms,start_ms,end_ms,finish_ms"
    )?;
    writeln!(
        inputs,
        "scenario,repetition,seed,task_id,zone,duration_ms,release_ms"
    )?;
    let mut order = 0;
    let mut order_rng = StdRng::seed_from_u64(c.seed ^ 0xabcdef);
    for repetition in 1..=c.repetitions {
        let mut cases = Vec::new();
        for (scenario_index, scenario) in SCENARIOS.iter().enumerate() {
            let seed = c.seed + u64::from(repetition) * 100 + scenario_index as u64;
            let (zones, scheduled) = workload(scenario, c.tasks, seed);
            for t in &scheduled {
                writeln!(
                    inputs,
                    "{scenario},{repetition},{seed},{},{},{},{}",
                    t.task.id, t.task.zone, t.task.duration_ms, t.release_ms
                )?;
            }
            for workers in [1, 3, 6] {
                for (name, policy) in POLICIES {
                    cases.push((
                        *scenario,
                        name,
                        policy,
                        workers,
                        zones,
                        seed,
                        scheduled.clone(),
                    ));
                }
            }
        }
        cases.shuffle(&mut order_rng);
        for (scenario, name, policy, workers, zones, seed, scheduled) in cases {
            order += 1;
            let run = run_once(&scheduled, zones, workers, policy);
            writeln!(
                raw,
                "{order},{scenario},{name},{workers},{repetition},{seed},{},{zones},{:.6},{:.6},{:.6},{:.6},{},{},{:.8},{},{},{},{},{}",
                c.tasks,
                run.elapsed_ms,
                run.throughput,
                run.p95_response_ms,
                run.p95_wait_ms,
                run.denials,
                run.attempts,
                run.fairness,
                run.completed,
                run.remaining,
                run.missing,
                run.duplicates,
                run.overlaps
            )?;
            for e in run.events {
                writeln!(
                    traces,
                    "{order},{scenario},{name},{workers},{repetition},{},{},{},{},{:.6},{:.6},{:.6},{:.6}",
                    e.task_id,
                    e.zone,
                    e.duration_ms,
                    e.worker,
                    e.arrival_ms,
                    e.start_ms,
                    e.end_ms,
                    e.finish_ms
                )?;
            }
            raw.flush()?;
            traces.flush()?;
            println!(
                "{order}/{} {scenario} {name} workers={workers}: {:.2} tasks/s; denials={}",
                c.repetitions * 54,
                run.throughput,
                run.denials
            );
        }
    }
    inputs.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn workload_specs_have_expected_skew_arrivals_and_zone_capacity() {
        let (zones, tasks) = workload("pharmacy_hotspot", 120, 7);
        assert_eq!(zones, 3);
        assert_eq!(tasks.iter().filter(|t| t.task.zone == 0).count(), 96);
        let (_, bursts) = workload("burst_requests", 120, 7);
        for release in [0, 200, 400] {
            assert_eq!(
                bursts.iter().filter(|t| t.release_ms == release).count(),
                40
            );
        }
        let (zones, tasks) = workload("multi_ward", 120, 7);
        assert_eq!(zones, 6);
        for zone in 0..6 {
            assert_eq!(tasks.iter().filter(|t| t.task.zone == zone).count(), 20);
        }
    }
    #[test]
    fn percentile_uses_nearest_rank() {
        assert_eq!(p95((1..=20).map(f64::from).collect()), 19.0);
    }
}
