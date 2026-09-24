use robot_coordination::{DispatchOutcome, Robot, RobotCoordinator, Task};
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_REPETITIONS: u32 = 10;
const DEFAULT_TASKS: u32 = 60;
const DEFAULT_ZONES: u32 = 3;
const DEFAULT_DURATION_MS: u64 = 20;
const DEFAULT_MAX_ROBOTS: u32 = 6;

#[derive(Debug, Clone)]
struct Config {
    repetitions: u32,
    tasks: u32,
    zones: u32,
    duration_ms: u64,
    max_robots: u32,
    output: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            repetitions: DEFAULT_REPETITIONS,
            tasks: DEFAULT_TASKS,
            zones: DEFAULT_ZONES,
            duration_ms: DEFAULT_DURATION_MS,
            max_robots: DEFAULT_MAX_ROBOTS,
            output: PathBuf::from("../benchmark-results/local"),
        }
    }
}

#[derive(Debug, Clone)]
struct RunResult {
    run: u32,
    robots: u32,
    tasks: u32,
    zones: u32,
    task_duration_ms: u64,
    elapsed_ms: f64,
    throughput_tasks_s: f64,
    p95_completion_latency_ms: f64,
    zone_denials: u32,
    completed: u32,
    remaining: usize,
    jain_fairness: f64,
}

fn usage() -> &'static str {
    "TARDEM reproducible scalability benchmark\n\
\n\
Usage: cargo run --release --bin benchmark -- [OPTIONS]\n\
\n\
Options:\n\
  --repetitions N   Runs per robot count (default: 10)\n\
  --tasks N         Tasks per run (default: 60)\n\
  --zones N         Exclusive zones (default: 3)\n\
  --duration-ms N   Simulated work per task (default: 20)\n\
  --max-robots N    Test robot counts from 1 through N (default: 6)\n\
  --output PATH     CSV output directory (default: ../benchmark-results/local)\n\
  -h, --help        Show this help"
}

fn parse_positive<T>(flag: &str, value: Option<String>) -> Result<T, String>
where
    T: std::str::FromStr + PartialEq + Default,
{
    let raw = value.ok_or_else(|| format!("{flag} requires a value"))?;
    let parsed = raw
        .parse::<T>()
        .map_err(|_| format!("invalid value for {flag}: {raw}"))?;
    if parsed == T::default() {
        return Err(format!("{flag} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut config = Config::default();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--repetitions" => config.repetitions = parse_positive(&arg, args.next())?,
            "--tasks" => config.tasks = parse_positive(&arg, args.next())?,
            "--zones" => config.zones = parse_positive(&arg, args.next())?,
            "--duration-ms" => config.duration_ms = parse_positive(&arg, args.next())?,
            "--max-robots" => config.max_robots = parse_positive(&arg, args.next())?,
            "--output" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--output requires a path".to_string())?;
                config.output = PathBuf::from(value);
            }
            _ => return Err(format!("unknown option: {arg}\n\n{}", usage())),
        }
    }

    Ok(Some(config))
}

fn percentile_95(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let index = ((values.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
    values[index]
}

fn jain_fairness(completions: &[u32]) -> f64 {
    let sum: f64 = completions.iter().map(|&value| f64::from(value)).sum();
    let squared_sum: f64 = completions
        .iter()
        .map(|&value| f64::from(value).powi(2))
        .sum();

    if squared_sum == 0.0 {
        1.0
    } else {
        sum.powi(2) / (completions.len() as f64 * squared_sum)
    }
}

fn run_once(config: &Config, robots: u32, run: u32) -> RunResult {
    let coordinator = Arc::new(RobotCoordinator::new(config.zones));
    for robot_id in 0..robots {
        coordinator
            .register_robot(Robot::new(robot_id, &format!("BenchmarkRobot-{robot_id}")))
            .expect("benchmark robot IDs are unique");
    }

    for task_id in 0..config.tasks {
        coordinator.add_task(Task::new(
            task_id,
            task_id % config.zones,
            config.duration_ms,
            "deterministic benchmark task",
        ));
    }

    let barrier = Arc::new(Barrier::new(robots as usize + 1));
    let start = Arc::new(OnceLock::<Instant>::new());
    let completion_latencies =
        Arc::new(Mutex::new(Vec::<f64>::with_capacity(config.tasks as usize)));
    let completions = Arc::new((0..robots).map(|_| Mutex::new(0_u32)).collect::<Vec<_>>());
    let mut handles = Vec::with_capacity(robots as usize);

    for robot_id in 0..robots {
        let coordinator = Arc::clone(&coordinator);
        let barrier = Arc::clone(&barrier);
        let start = Arc::clone(&start);
        let completion_latencies = Arc::clone(&completion_latencies);
        let completions = Arc::clone(&completions);

        handles.push(thread::spawn(move || {
            barrier.wait();
            let benchmark_start = *start.get().expect("start time set before barrier release");

            loop {
                coordinator.heartbeat(robot_id);
                match coordinator
                    .dispatch_next_task(robot_id)
                    .expect("registered benchmark robot remains online")
                {
                    DispatchOutcome::Assigned { task, zone_guard } => {
                        thread::sleep(Duration::from_millis(task.duration_ms));
                        assert!(coordinator.leave_zone(zone_guard, robot_id, task.zone as usize));
                        coordinator
                            .complete_task(robot_id)
                            .expect("assigned task can be completed");
                        *completions[robot_id as usize]
                            .lock()
                            .expect("completion counter lock") += 1;
                        completion_latencies
                            .lock()
                            .expect("latency vector lock")
                            .push(benchmark_start.elapsed().as_secs_f64() * 1_000.0);
                    }
                    DispatchOutcome::ZoneBusy { .. } => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    DispatchOutcome::QueueEmpty => break,
                }
            }
        }));
    }

    start
        .set(Instant::now())
        .expect("benchmark start time is set once");
    barrier.wait();
    for handle in handles {
        handle.join().expect("benchmark worker should not panic");
    }
    let elapsed_ms = start
        .get()
        .expect("benchmark start time available")
        .elapsed()
        .as_secs_f64()
        * 1_000.0;

    let stats = coordinator.get_stats();
    let remaining = coordinator.queue_length();
    let mut latencies = completion_latencies
        .lock()
        .expect("latency vector lock")
        .clone();
    let completion_counts: Vec<u32> = completions
        .iter()
        .map(|count| *count.lock().expect("completion counter lock"))
        .collect();

    assert_eq!(
        stats.total_tasks_completed, config.tasks,
        "every created task must complete"
    );
    assert_eq!(remaining, 0, "no task may remain queued");
    assert_eq!(latencies.len(), config.tasks as usize);

    RunResult {
        run,
        robots,
        tasks: config.tasks,
        zones: config.zones,
        task_duration_ms: config.duration_ms,
        elapsed_ms,
        throughput_tasks_s: f64::from(config.tasks) / (elapsed_ms / 1_000.0),
        p95_completion_latency_ms: percentile_95(&mut latencies),
        zone_denials: stats.total_zone_denials,
        completed: stats.total_tasks_completed,
        remaining,
        jain_fairness: jain_fairness(&completion_counts),
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn sample_standard_deviation(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let average = mean(values);
    let squared_error = values
        .iter()
        .map(|value| (value - average).powi(2))
        .sum::<f64>();
    (squared_error / (values.len() - 1) as f64).sqrt()
}

fn write_results(config: &Config, results: &[RunResult]) -> std::io::Result<()> {
    fs::create_dir_all(&config.output)?;

    let raw_path = config.output.join("raw_runs.csv");
    let mut raw = BufWriter::new(File::create(&raw_path)?);
    writeln!(
        raw,
        "run,robots,tasks,zones,task_duration_ms,elapsed_ms,throughput_tasks_s,p95_completion_latency_ms,zone_denials,completed,remaining,jain_fairness"
    )?;
    for result in results {
        writeln!(
            raw,
            "{},{},{},{},{},{:.3},{:.3},{:.3},{},{},{},{:.6}",
            result.run,
            result.robots,
            result.tasks,
            result.zones,
            result.task_duration_ms,
            result.elapsed_ms,
            result.throughput_tasks_s,
            result.p95_completion_latency_ms,
            result.zone_denials,
            result.completed,
            result.remaining,
            result.jain_fairness
        )?;
    }

    let summary_path = config.output.join("summary.csv");
    let mut summary = BufWriter::new(File::create(&summary_path)?);
    writeln!(
        summary,
        "robots,runs,mean_elapsed_ms,sd_elapsed_ms,mean_throughput_tasks_s,sd_throughput_tasks_s,mean_p95_completion_latency_ms,mean_zone_denials,mean_jain_fairness,successful_runs"
    )?;

    for robots in 1..=config.max_robots {
        let group: Vec<&RunResult> = results
            .iter()
            .filter(|result| result.robots == robots)
            .collect();
        let elapsed: Vec<f64> = group.iter().map(|result| result.elapsed_ms).collect();
        let throughput: Vec<f64> = group
            .iter()
            .map(|result| result.throughput_tasks_s)
            .collect();
        let p95_latency: Vec<f64> = group
            .iter()
            .map(|result| result.p95_completion_latency_ms)
            .collect();
        let denials: Vec<f64> = group
            .iter()
            .map(|result| f64::from(result.zone_denials))
            .collect();
        let fairness: Vec<f64> = group.iter().map(|result| result.jain_fairness).collect();
        let successful_runs = group
            .iter()
            .filter(|result| result.completed == result.tasks && result.remaining == 0)
            .count();

        writeln!(
            summary,
            "{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.6},{}",
            robots,
            group.len(),
            mean(&elapsed),
            sample_standard_deviation(&elapsed),
            mean(&throughput),
            sample_standard_deviation(&throughput),
            mean(&p95_latency),
            mean(&denials),
            mean(&fairness),
            successful_runs
        )?;
    }

    raw.flush()?;
    summary.flush()?;
    println!("Raw runs: {}", raw_path.display());
    println!("Summary:  {}", summary_path.display());
    Ok(())
}

fn main() {
    let config = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("{}", usage());
            return;
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };

    println!("TARDEM scalability benchmark");
    println!(
        "{} deterministic tasks, {} zones, {} ms task duration, {} repetitions",
        config.tasks, config.zones, config.duration_ms, config.repetitions
    );
    println!(
        "Available parallelism: {}",
        thread::available_parallelism().map_or(1, usize::from)
    );

    let mut results = Vec::with_capacity((config.max_robots * config.repetitions) as usize);
    for robots in 1..=config.max_robots {
        for run in 1..=config.repetitions {
            let result = run_once(&config, robots, run);
            println!(
                "robots={robots} run={run:02} elapsed={:.3} ms throughput={:.3} tasks/s denials={} fairness={:.4}",
                result.elapsed_ms,
                result.throughput_tasks_s,
                result.zone_denials,
                result.jain_fairness
            );
            results.push(result);
        }
    }

    if let Err(error) = write_results(&config, &results) {
        eprintln!("failed to write benchmark results: {error}");
        std::process::exit(1);
    }
}
