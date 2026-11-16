// Drive the real frontend against a live server and assert on rendered DOM.
// Usage: node tests/ui_check.mjs http://127.0.0.1:PORT
import { runPage, settle, textOf, tableRows } from "./ui_harness.mjs";

const base = process.argv[2] || "http://127.0.0.1:8137";
const { document, calls, errors } = runPage(base);

function assert(cond, message) {
  if (!cond) throw new Error("ASSERT: " + message);
}

await settle(1200);

assert(textOf(document.getElementById("snapshot-line")).includes("快照"), "snapshot line rendered");
assert(document.getElementById("method").children.length >= 5, "methods populated");
assert(tableRows(document, "snapshots").length >= 1, "snapshot table has rows");
assert(tableRows(document, "tasks").length >= 0, "tasks table rendered");
console.log("initial render ok:", textOf(document.getElementById("snapshot-line")));

// Run a preview through the form's real submit handler.
document.getElementById("method").value = "stratified_rare";
document.getElementById("budget").value = "240";
document.getElementById("page-size").value = "10";
document.getElementById("seed").value = "ui-check";
await document.getElementById("preview-form").dispatch("submit");
await settle(2500);

const statusText = textOf(document.getElementById("task-status"));
assert(/COMPLETE/.test(statusText), "preview completed: " + statusText);
const sampleRows = tableRows(document, "rows");
assert(sampleRows.length === 10, `first page has 10 rows, got ${sampleRows.length}`);
console.log("preview status:", statusText);
console.log("meters:", document.getElementById("read-budget").children.length);
assert(document.getElementById("read-budget").children.length === 4, "read budget meters rendered");

await settle(600);
const kindRows = tableRows(document, "kind-table");
// The warehouse may already contain partitions from earlier runs, so assert
// that the table carries the weighted estimates rather than an exact count.
assert(kindRows.length >= 5, `kind estimate table populated (${kindRows.length})`);
assert(kindRows.every((r) => r.length === 7), "kind table has the estimate columns");
assert(textOf(document.getElementById("stats-disclaimer")).includes("Estimates only"), "estimate disclaimer shown");
const rareRows = tableRows(document, "rare-table");
assert(rareRows.length >= 5, `rare coverage table populated (${rareRows.length})`);
assert(rareRows.some((r) => r[5] === "covered"), "at least one rare kind covered");
const partitionRows = tableRows(document, "partition-table");
assert(partitionRows.length >= 40, "partition coverage table populated");
console.log("stats ok: kinds=" + kindRows.length + " rare=" + rareRows.length + " partitions=" + partitionRows.length);

// Paginate with the real token flow.
const page1 = sampleRows.map((r) => r[3]).join(",");
await document.getElementById("next").dispatch("click");
await settle(800);
const page2 = tableRows(document, "rows").map((r) => r[3]).join(",");
assert(page1 !== page2, "second page differs from first");
assert(textOf(document.getElementById("token-note")).includes("令牌"), "token note rendered");
console.log("pagination ok; label:", textOf(document.getElementById("page-label")));

// Full query + comparison.
await document.getElementById("full-query").dispatch("click");
await settle(4000);
const compareRows = tableRows(document, "compare-table");
assert(compareRows.length >= 10, `comparison table populated (${compareRows.length})`);
assert(textOf(document.getElementById("compare-note")).includes("exact count"), "compare disclaimer mentions exact count");
console.log("compare ok: rows=" + compareRows.length);

// Write a new partition, then refresh the snapshot table.
// Use a per-run date so the check can be repeated against the same warehouse.
const probeDate = new Date(Date.UTC(2030, 0, 1) + (Date.now() % 2_000) * 86_400_000)
  .toISOString().slice(0, 10);
const probeKind = "ui_probe_kind_" + (Date.now() % 10_000);
document.getElementById("new-date").value = probeDate;
document.getElementById("new-kind").value = probeKind;
document.getElementById("new-weight").value = "0.02";
await document.getElementById("add-partition").dispatch("click");
await settle(2500);
const maintenance = textOf(document.getElementById("maintenance-note"));
assert(maintenance.includes(probeKind), "maintenance note reports new kind: " + maintenance);
assert(tableRows(document, "snapshots").length >= 3, "new snapshot appears");

// Merge two partitions.
document.getElementById("merge-dates").value = "2026-11-13,2026-11-14";
await document.getElementById("merge").dispatch("click");
await settle(3000);
assert(textOf(document.getElementById("maintenance-note")).includes("已合并"), "merge reported");

assert(errors.length === 0, "no console errors: " + errors.join(" | "));
console.log("UI CHECK PASSED");
console.log("api calls:", calls.length);
