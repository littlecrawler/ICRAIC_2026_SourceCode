import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';

const directory = process.argv[2] || 'benchmark-results';
const paperDirectory = process.argv[3];
const readCsv = name => {
  const [header, ...lines] = fs.readFileSync(path.join(directory, name), 'utf8').trim().split(/\r?\n/);
  const fields = header.split(',');
  return lines.map(line => {
    const cells = line.split(',').map(Number);
    assert.equal(cells.length, fields.length);
    assert.ok(cells.every(Number.isFinite));
    return Object.fromEntries(fields.map((field, index) => [field, cells[index]]));
  });
};
const mean = values => values.reduce((sum, value) => sum + value, 0) / values.length;
const sd = values => Math.sqrt(values.reduce((sum, value) => sum + (value - mean(values)) ** 2, 0) / (values.length - 1));
const close = (actual, expected, tolerance, label) => assert.ok(Math.abs(actual - expected) <= tolerance, `${label}: ${actual} vs ${expected}`);
const rows = readCsv('raw_runs.csv');
const summary = readCsv('summary.csv').sort((a, b) => a.robots - b.robots);
assert.equal(rows.length, 60);
assert.deepEqual(summary.map(row => row.robots), [1, 2, 3, 4, 5, 6]);
assert.equal(new Set(rows.map(row => `${row.robots}/${row.run}`)).size, 60);
for (const row of rows) {
  assert.ok(Number.isInteger(row.robots) && row.robots >= 1 && row.robots <= 6);
  assert.ok(Number.isInteger(row.run) && row.run >= 1 && row.run <= 10);
  assert.equal(row.tasks, 60);
  assert.equal(row.zones, 3);
  assert.equal(row.task_duration_ms, 20);
  assert.equal(row.completed, 60);
  assert.equal(row.remaining, 0);
  assert.ok(Number.isInteger(row.zone_denials) && row.zone_denials >= 0);
  assert.ok(row.elapsed_ms > 0 && row.p95_completion_latency_ms > 0 && row.p95_completion_latency_ms <= row.elapsed_ms);
  assert.ok(row.jain_fairness >= 1 / row.robots - 0.000001 && row.jain_fairness <= 1);
  close(row.throughput_tasks_s, row.completed * 1000 / row.elapsed_ms, 0.001, 'rounded throughput');
}
for (const row of summary) {
  const group = rows.filter(run => run.robots === row.robots);
  assert.equal(group.length, 10);
  assert.equal(row.runs, 10);
  assert.equal(row.successful_runs, 10);
  // Both input files retain rounded measurements, so comparisons allow their rounding error.
  for (const metric of ['elapsed_ms', 'throughput_tasks_s', 'p95_completion_latency_ms', 'zone_denials', 'jain_fairness']) {
    close(mean(group.map(run => run[metric])), row[`mean_${metric}`], metric === 'jain_fairness' ? 0.0000015 : 0.0015, `mean ${metric}`);
  }
  for (const metric of ['elapsed_ms', 'throughput_tasks_s']) {
    close(sd(group.map(run => run[metric])), row[`sd_${metric}`], 0.0015, `sample SD ${metric}`);
  }
}
const audit = {
  runs: rows.length,
  task_completions_from_counters: rows.reduce((sum, row) => sum + row.completed, 0),
  residual_tasks: rows.reduce((sum, row) => sum + row.remaining, 0),
  summary_matches_rounded_raw_measurements: true,
  task_identity_traces_available: false,
  pooled_with_new_study: false,
};
console.log(JSON.stringify(audit));

if (paperDirectory) {
  let table = String.raw`\begin{table}[t]
\caption{Original tail-retry scalability: ten runs per worker count, 60 tasks, three zones, and 20-ms service. Values are means; throughput also reports sample SD.}
\label{tab:original}
\centering\small
\setlength{\tabcolsep}{4pt}
\begin{tabular}{crrr}
\toprule
\textbf{Workers} & \textbf{Throughput (tasks/s)} & \textbf{Denials} & \textbf{Jain $J$}\\
\midrule
`;
  for (const row of summary) {
    table += `${row.robots} & $${row.mean_throughput_tasks_s.toFixed(3)} \\pm ${row.sd_throughput_tasks_s.toFixed(3)}$ & ${row.mean_zone_denials.toFixed(1)} & ${row.mean_jain_fairness.toFixed(4)} \\\\\n`;
  }
  table += '\\bottomrule\n\\end{tabular}\n\\end{table}\n';
  fs.mkdirSync(paperDirectory, { recursive: true });
  fs.writeFileSync(path.join(paperDirectory, 'original_scalability.tex'), table);
}
