// ─────────────────────────────────────────────────────────────
// MongoDB V8 – Storage Stats Report
// Run with:
//   mongosh "mongodb://localhost:27017" --file storage_stats_report.js
//   mongosh "mongodb://localhost:27017" --file storage_stats_report.js \
//           --eval 'var DB="perf_test"; var COLL="bulk_delete_test";'
// ─────────────────────────────────────────────────────────────

// ── Config (override via --eval before --file) ───────────────
if (typeof DB   === "undefined") var DB   = "perf_test";
if (typeof COLL === "undefined") var COLL = "bulk_delete_test";

// ── Helpers ──────────────────────────────────────────────────
function fmtBytes(bytes) {
    if (bytes === undefined || bytes === null) return "N/A";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let v = bytes;
    for (let u of units) {
        if (Math.abs(v) < 1024) return v.toFixed(2) + " " + u;
        v /= 1024;
    }
    return v.toFixed(2) + " PB";
}

function fmtNum(n) {
    if (n === undefined || n === null) return "N/A";
    return n.toLocaleString();
}

function pad(str, width) {
    str = String(str);
    return str.length >= width ? str : " ".repeat(width - str.length) + str;
}

function line(label, value, note) {
    let row = "  " + (label + " ").padEnd(40, ".") + " " + pad(value, 16);
    if (note) row += "   // " + note;
    print(row);
}

function divider(char) { print("  " + (char || "─").repeat(64)); }

// ── Pull Stats ───────────────────────────────────────────────
var db      = db.getSiblingDB(DB);
var rawColl = db.runCommand({ collStats: COLL });
var rawDb   = db.runCommand({ dbStats: 1, scale: 1 });
var wt      = rawColl.wiredTiger || {};
var bm      = wt["block-manager"] || {};
var cache   = (wt["cache"] || {});

// ── Print Report ─────────────────────────────────────────────
print("");
print("  ═══════════════════════════════════════════════════════════════");
print("    MongoDB Storage Stats Report  |  " + new Date().toISOString());
print("  ═══════════════════════════════════════════════════════════════");
print("    Database   : " + DB);
print("    Collection : " + COLL);
print("  ═══════════════════════════════════════════════════════════════");

// ── Document Counts ──────────────────────────────────────────
print("");
print("  DOCUMENT COUNTS");
divider();
line("Document count",         fmtNum(rawColl.count),        "live documents in collection");
line("Avg document size",      fmtBytes(rawColl.avgObjSize), "uncompressed average");

// ── On-Disk Storage ──────────────────────────────────────────
print("");
print("  ON-DISK STORAGE  (collection)");
divider();
line("Logical size (docs)",    fmtBytes(rawColl.size),          "uncompressed document bytes");
line("Storage size (data)",    fmtBytes(rawColl.storageSize),   "compressed, on disk  ← key metric");
line("Total index size",       fmtBytes(rawColl.totalIndexSize),"all indexes combined");
line("Total size (data+idx)",  fmtBytes(rawColl.totalSize),     "storageSize + totalIndexSize");
line("Capped",                 String(rawColl.capped || false));

// ── WiredTiger Internals ─────────────────────────────────────
print("");
print("  WIREDTIGER INTERNALS");
divider();
line("WT file size on disk",   fmtBytes(bm["file size in bytes"]),
     "physical .wt file size");
line("WT reusable bytes",      fmtBytes(bm["file bytes available for reuse"]),
     "freed but not yet returned to OS");
line("WT bytes written",       fmtBytes(bm["bytes written"]),   "cumulative writes");
line("WT bytes read",          fmtBytes(bm["bytes read"]),      "cumulative reads");
line("WT pages rewritten",     fmtNum(bm["pages written"]),     "WT page write ops");

var reuseBytes = bm["file bytes available for reuse"] || 0;
var fileBytes  = bm["file size in bytes"] || 1;
var reusePct   = (reuseBytes / fileBytes * 100).toFixed(1);
print("");
line("Space fragmentation",    reusePct + "%",
     "reusable / file size  (0% = fully compacted)");

// ── Index Detail ─────────────────────────────────────────────
print("");
print("  INDEX BREAKDOWN");
divider();
var idxSizes = rawColl.indexSizes || {};
for (var idxName in idxSizes) {
    line(idxName, fmtBytes(idxSizes[idxName]));
}

// ── Database-Level Stats ─────────────────────────────────────
print("");
print("  DATABASE-LEVEL  (" + DB + ")");
divider();
line("Collections",            fmtNum(rawDb.collections));
line("DB data size",           fmtBytes(rawDb.dataSize),       "all collections, uncompressed");
line("DB storage size",        fmtBytes(rawDb.storageSize),    "all collections, on disk");
line("DB index size",          fmtBytes(rawDb.indexSize));
line("DB total size",          fmtBytes(rawDb.totalSize));
line("DB objects",             fmtNum(rawDb.objects));

// ── Compaction Hint ──────────────────────────────────────────
print("");
divider("═");
if (reuseBytes > 0) {
    print("  ⚠  " + fmtBytes(reuseBytes) + " (" + reusePct +
          "%) of the WT file is reusable but not yet");
    print("     returned to the OS. To reclaim it:");
    print("       db.runCommand({ compact: '" + COLL + "' })           // foreground");
    print("       db.adminCommand({ setParameter:1, autoCompact:true }) // background (MDB 8)");
} else {
    print("  ✅  No significant fragmentation detected.");
}
divider("═");
print("");
