import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';

const directory = process.argv[2] || 'benchmark-results/revision-20261006';
const paperDirectory = process.argv[3];
const readCsv = name => {
  const [header, ...lines] = fs.readFileSync(path.join(directory, name), 'utf8').trim().split(/\r?\n/);
  const fields = header.split(',');
  return lines.map(line => Object.fromEntries(line.split(',').map((v,i) => [fields[i], v !== '' && Number.isFinite(Number(v)) ? Number(v) : v])));
};
const csv = (name, rows) => {
  const fields = Object.keys(rows[0]);
  fs.writeFileSync(path.join(directory,name), [fields.join(','), ...rows.map(r => fields.map(k => r[k]).join(','))].join('\n')+'\n');
};
const mean = x => x.reduce((a,b)=>a+b,0)/x.length;
const sd = x => x.length<2 ? 0 : Math.sqrt(x.reduce((a,b)=>a+(b-mean(x))**2,0)/(x.length-1));
const p95 = x => [...x].sort((a,b)=>a-b)[Math.ceil(x.length*.95)-1];
const close = (a,b,what) => assert.ok(Math.abs(a-b)<.00002, `${what}: ${a} vs ${b}`);
const key = r => `${r.scenario}/${r.policy}/${r.workers}`;
const rows = readCsv('raw_runs.csv');
const events = readCsv('task_events.csv');
const workloads = readCsv('workloads.csv');
const grouped = Map.groupBy(rows, key);
const byRun = Map.groupBy(events, e=>e.order);
const byWorkload = Map.groupBy(workloads, e=>`${e.scenario}/${e.repetition}`);
assert.equal(new Set(rows.map(r=>r.order)).size, rows.length);
assert.equal(new Set(rows.map(r=>`${key(r)}/${r.repetition}`)).size,rows.length);
assert.equal(byRun.size, rows.length);
const repetitions = Math.max(...rows.map(r=>r.repetition));
assert.equal(rows.length, 54*repetitions);
assert.equal(grouped.size,54);
for (const group of grouped.values()) assert.equal(group.length,repetitions);
for (const r of rows) {
  const e = byRun.get(r.order);
  const input = byWorkload.get(`${r.scenario}/${r.repetition}`);
  assert.equal(e.length, r.tasks);
  assert.equal(input.length,r.tasks);
  assert.equal(new Set(e.map(t=>t.task_id)).size,r.tasks);
  const specification = new Map(input.map(t=>[t.task_id,t]));
  for(const t of e) {
    assert.equal(t.scenario,r.scenario); assert.equal(t.policy,r.policy);
    assert.equal(t.workers,r.workers); assert.equal(t.repetition,r.repetition);
    assert.ok(t.worker>=0 && t.worker<r.workers);
    const expected=specification.get(t.task_id); assert.ok(expected);
    assert.equal(t.zone,expected.zone); assert.equal(t.duration_ms,expected.duration_ms);
    assert.equal(expected.seed,r.seed);
    assert.ok(t.arrival_ms+.01>=expected.release_ms);
    assert.ok(t.arrival_ms<=t.start_ms && t.start_ms<=t.end_ms && t.end_ms<=t.finish_ms);
    assert.ok(t.end_ms-t.start_ms+.01>=t.duration_ms);
  }
  for(const intervals of Map.groupBy(e,t=>t.zone).values()) {
    intervals.sort((a,b)=>a.start_ms-b.start_ms);
    assert.ok(intervals.slice(1).every((t,i)=>intervals[i].end_ms<=t.start_ms));
  }
  const elapsed=Math.max(...e.map(t=>t.finish_ms));
  close(elapsed,r.elapsed_ms,'makespan');
  close(r.tasks*1000/elapsed,r.throughput_tasks_s,'throughput');
  close(p95(e.map(t=>t.finish_ms-t.arrival_ms)),r.p95_response_ms,'response');
  close(p95(e.map(t=>t.start_ms-t.arrival_ms)),r.p95_wait_ms,'wait');
  const counts=Array(r.workers).fill(0); for(const t of e) counts[t.worker]++;
  close(r.tasks**2/(r.workers*counts.reduce((a,b)=>a+b*b,0)),r.jain_fairness,'fairness');
  assert.equal(r.completed,r.tasks);
  for(const k of ['remaining','missing','duplicates','overlaps']) assert.equal(r[k],0);
  assert.equal(r.zone_attempts,r.zone_denials+r.tasks);
}
const metrics=['elapsed_ms','throughput_tasks_s','p95_response_ms','p95_wait_ms','zone_denials','zone_attempts','jain_fairness'];
const summary=[...grouped.entries()].map(([k,g])=> {
  const [scenario,policy,workers]=k.split('/');
  const out={scenario,policy,workers:Number(workers),repetitions:g.length,tasks:g[0].tasks};
  for(const metric of metrics) {
    const values=g.map(r=>r[metric]);
    out[`${metric}_mean`]=mean(values);
    out[`${metric}_sd`]=sd(values);
  }
  return out;
}).sort((a,b)=>key(a).localeCompare(key(b)));
csv('summary.csv',summary);

let state=20261006;
function random(){state^=state<<13;state^=state>>>17;state^=state<<5;return (state>>>0)/4294967296;}
const comparisons=[];
for(const scenario of [...new Set(rows.map(r=>r.scenario))].sort()) {
  for(const workers of [1,3,6]) {
    for(const baseline of ['fifo_blocking','retry_tail']) {
      const s=grouped.get(`${scenario}/ready_scan/${workers}`).toSorted((a,b)=>a.repetition-b.repetition);
      const b=grouped.get(`${scenario}/${baseline}/${workers}`).toSorted((a,b)=>a.repetition-b.repetition);
      assert.deepEqual(s.map(r=>r.seed),b.map(r=>r.seed));
      const ratio=s.map((r,i)=>r.throughput_tasks_s/b[i].throughput_tasks_s);
      const draws=Array.from({length:10000},()=>mean(ratio.map(()=>ratio[Math.floor(random()*ratio.length)]))).sort((a,b)=>a-b);
      comparisons.push({scenario,workers,baseline,repetitions:s.length,mean_paired_throughput_ratio:mean(ratio),bootstrap95_low:draws[249],bootstrap95_high:draws[9749]});
    }
  }
}
csv('paired_comparisons.csv',comparisons);
const audit={runs:rows.length,tasks:events.length,configurations:grouped.size,repetitions,missing:0,duplicates:0,overlapping_recorded_intervals:0,metrics_reconstructed:true,inputs_verified:true};
fs.writeFileSync(path.join(directory,'validation.json'),JSON.stringify(audit,null,2)+'\n');
console.log(JSON.stringify(audit));
const get=(s,p,w)=>summary.find(r=>r.scenario===s&&r.policy===p&&r.workers===w);
const comp=(s,w,b='fifo_blocking')=>comparisons.find(r=>r.scenario===s&&r.workers===w&&r.baseline===b);
console.table(summary.filter(r=>r.workers===3).map(r=>({scenario:r.scenario,policy:r.policy,X:r.throughput_tasks_s_mean.toFixed(2),SD:r.throughput_tasks_s_sd.toFixed(2),p95:r.p95_response_ms_mean.toFixed(1)})));
console.table(comparisons.filter(r=>r.workers===3&&r.baseline==='fifo_blocking'));

if(paperDirectory) {
  const f=(n,d=2)=>Number(n).toFixed(d);
  const values={
    ClusterRatio:f(comp('clustered_rounds',3).mean_paired_throughput_ratio),ClusterLow:f(comp('clustered_rounds',3).bootstrap95_low),ClusterHigh:f(comp('clustered_rounds',3).bootstrap95_high),
    HotRatio:f(comp('pharmacy_hotspot',3).mean_paired_throughput_ratio),HotLow:f(comp('pharmacy_hotspot',3).bootstrap95_low),HotHigh:f(comp('pharmacy_hotspot',3).bootstrap95_high),
    ClusterScan:f(get('clustered_rounds','ready_scan',3).throughput_tasks_s_mean),ClusterFifo:f(get('clustered_rounds','fifo_blocking',3).throughput_tasks_s_mean),ClusterRetry:f(get('clustered_rounds','retry_tail',3).throughput_tasks_s_mean),
    MultiScan:f(get('multi_ward','ready_scan',6).throughput_tasks_s_mean),MultiFifo:f(get('multi_ward','fifo_blocking',6).throughput_tasks_s_mean),MultiRetry:f(get('multi_ward','retry_tail',6).throughput_tasks_s_mean),
    ClusterLatencyScan:f(get('clustered_rounds','ready_scan',3).p95_response_ms_mean,1),ClusterLatencyFifo:f(get('clustered_rounds','fifo_blocking',3).p95_response_ms_mean,1),
    BurstLatencyScan:f(get('burst_requests','ready_scan',3).p95_response_ms_mean,1),BurstLatencyFifo:f(get('burst_requests','fifo_blocking',3).p95_response_ms_mean,1),
    MixedDenialsScan:f(get('mixed_services','ready_scan',6).zone_denials_mean/get('mixed_services','ready_scan',6).tasks),MixedDenialsRetry:f(get('mixed_services','retry_tail',6).zone_denials_mean/get('mixed_services','retry_tail',6).tasks),
    MixedFairScan:f(get('mixed_services','ready_scan',6).jain_fairness_mean,3),MixedFairRetry:f(get('mixed_services','retry_tail',6).jain_fairness_mean,3),
  };
  fs.mkdirSync(paperDirectory,{recursive:true});
  fs.writeFileSync(path.join(paperDirectory,'revision_numbers.tex'),Object.entries(values).map(([k,v])=>`\\newcommand{\\${k}}{${v}}`).join('\n')+'\n');
  const labels={ward_delivery:'Ward delivery',pharmacy_hotspot:'Pharmacy hotspot',mixed_services:'Mixed services',clustered_rounds:'Clustered rounds',burst_requests:'Burst requests',multi_ward:'Multi-ward'};
  let table=String.raw`\begin{table*}[t]
\caption{Three-worker policy comparison across ten paired repetitions per workload. Throughput is mean $\pm$ sample SD; response p95 is the mean of run-level percentiles. The final column is the mean paired S/B throughput ratio with its bootstrap 95\% interval.}
\label{tab:comparison}
\centering\small
\begin{tabular}{lrrrccc}
\toprule
& \multicolumn{3}{c}{\textbf{Throughput (tasks/s)}} & \multicolumn{2}{c}{\textbf{Response p95 (ms)}} & \textbf{Paired ratio}\\
\textbf{Workload} & \textbf{B} & \textbf{R} & \textbf{S} & \textbf{B} & \textbf{S} & \textbf{S/B [95\% interval]}\\
\midrule
`;
  for(const [scenario,label] of Object.entries(labels)) {
    const g=['fifo_blocking','retry_tail','ready_scan'].map(p=>get(scenario,p,3));const c=comp(scenario,3);
    table+=`${label} & `+g.map(r=>`$${f(r.throughput_tasks_s_mean)} \\pm ${f(r.throughput_tasks_s_sd)}$`).join(' & ')+` & ${f(g[0].p95_response_ms_mean,1)} & ${f(g[2].p95_response_ms_mean,1)} & ${f(c.mean_paired_throughput_ratio)} [${f(c.bootstrap95_low)}, ${f(c.bootstrap95_high)}] \\\\\n`;
  }
  table+=String.raw`\bottomrule
\end{tabular}
\end{table*}
`;
  fs.writeFileSync(path.join(paperDirectory,'revision_comparison.tex'),table);
  let plot=String.raw`\begin{figure*}[t]
\centering
\begin{tikzpicture}
\begin{groupplot}[group style={group size=3 by 1,horizontal sep=1.15cm},width=0.29\textwidth,height=4.4cm,xlabel={Worker threads},ylabel={Throughput (tasks/s)},xtick={1,3,6},xmin=0.6,xmax=6.4,ymin=0,ymajorgrids,grid style={gray!20},tick label style={font=\scriptsize},label style={font=\scriptsize},title style={font=\small},legend style={font=\scriptsize,at={(0.02,0.97)},anchor=north west,draw=none,fill=white}]
`;
  for(const [i,scenario] of ['clustered_rounds','pharmacy_hotspot','multi_ward'].entries()) {
    plot+=`\\nextgroupplot[title={${labels[scenario]}},ymax=${scenario==='multi_ward'?650:scenario==='pharmacy_hotspot'?145:340}${i===0?',legend to name=policyLegend,legend columns=3':''}]\n`;
    for(const [j,policy] of ['fifo_blocking','retry_tail','ready_scan'].entries()) {
      plot+=`\\addplot+[${['black,mark=square*','orange!85!black,mark=triangle*','blue!75!black,mark=*'][j]},thick,error bars/.cd,y dir=both,y explicit] coordinates {`;
      plot+=[1,3,6].map(w=>{const r=get(scenario,policy,w);return `(${w},${f(r.throughput_tasks_s_mean)}) +- (0,${f(r.throughput_tasks_s_sd)})`;}).join(' ');
      plot+='};\n';if(i===0)plot+=`\\addlegendentry{${['B: blocking FIFO','R: tail retry','S: ready scan'][j]}}\n`;
    }
  }
  plot+=String.raw`\end{groupplot}
\end{tikzpicture}
\par\smallskip\ref{policyLegend}
\caption{Worker scaling under grouped orders, a shared-station hotspot, and six independent locations. Error bars show sample SD over ten runs.}
\label{fig:scaling}
\end{figure*}
`;
  fs.writeFileSync(path.join(paperDirectory,'revision_scaling.tex'),plot);
  fs.writeFileSync(path.join(paperDirectory,'revision_values.json'),JSON.stringify(values,null,2)+'\n');
}
