/**
 * Prints the Iceberg explorer's growth model as JSON: cumulative bytes on
 * storage per file kind over a week of one append per minute, no maintenance.
 *
 *     node bench/iceberg_growth.cjs [rowsPerCommit] [commits]
 */
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const EXPLORER = path.join(__dirname, "..", "explorer");
for (const f of ["common.js", "avro-codec.js", "parquet-codec.js", "iceberg-model.js"]) {
  vm.runInThisContext(fs.readFileSync(path.join(EXPLORER, f), "utf8"), { filename: f });
}
const IB = globalThis.DF.IB;

(async () => {
  const rowsPerCommit = Number(process.argv[2] || 100);
  const commits = Number(process.argv[3] || 10080);
  const cal = await IB.calibrate({ rowsPerCommit, maxCommits: commits });
  const series = IB.growth(cal, { commits, points: 600 });
  const maintained = await IB.maintained(cal, rowsPerCommit * commits);
  process.stdout.write(JSON.stringify({ rowsPerCommit, commits, series, maintained }));
})();
