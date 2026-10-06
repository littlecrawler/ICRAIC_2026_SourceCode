import fs from 'node:fs';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
const manifest=process.argv[2]||'benchmark-results/revision-20261006/SHA256SUMS.txt';
const lines=fs.readFileSync(manifest,'utf8').trim().split(/\r?\n/);
for(const line of lines){
  const match=line.match(/^([a-fA-F0-9]{64})  (.+)$/);assert.ok(match,`invalid manifest entry: ${line}`);
  const actual=crypto.createHash('sha256').update(fs.readFileSync(match[2])).digest('hex');
  assert.equal(actual,match[1].toLowerCase(),`hash mismatch: ${match[2]}`);
}
console.log(`Verified ${lines.length} snapshot files.`);
