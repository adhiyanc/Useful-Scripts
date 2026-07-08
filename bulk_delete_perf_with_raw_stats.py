#!/usr/bin/env python3
"""
MongoDB V8 Bulk Delete Performance Test
========================================
Purpose : Compare initial sync and auto compact disk-space release
 after a bulk delete of ~10M documents on a local MongoDB 8
 deployment.

Phases
------
1. (Optional) Create hashed index on `hashField`
2. Insert TOTAL_DOCS documents in batches
3. Capture storage stats → "AFTER INSERT"
4. Save raw collStats output after insert
5. Wait WAIT_SECONDS (default 300 s / 5 min)
6. Bulk-delete all documents
7. Capture storage stats → "AFTER DELETE (immediate)"
8. Save raw collStats output after delete
9. (Optional) Run compact() → "AFTER COMPACT"
10. Write JSON report

Document schema
---------------
{
 hashField : "<random 32-char hex>", # hashed index
 createdAt : ISODate, # timestamp 1
 updatedAt : ISODate, # timestamp 2
 customerId : "<uuid-style string>", # customer identifier
 x : 1,
 y : 2,
 z : 3,
 status : "active",
 region : "EU",
 priority : 5
}
"""

import os
import sys
import json
import time
import random
import string
import datetime
import argparse
import uuid
import pprint
from typing import Any, Dict, Tuple

try:
    import pymongo
    from pymongo import MongoClient
except ImportError:
    print("[ERROR] pymongo is not installed. Run: pip install pymongo")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION (override via CLI flags or edit directly here)
# ─────────────────────────────────────────────────────────────
DEFAULT_URI = "mongodb://localhost:27018"
DEFAULT_DB = "perf_test"
DEFAULT_COLLECTION = "bulk_delete_test_compact"
DEFAULT_TOTAL_DOCS = 70_000_00  # 7 M
DEFAULT_BATCH_SIZE = 70_000  # insertMany batch
DEFAULT_WAIT_SECONDS = 240  # 4 minutes between insert and delete
DEFAULT_REPORT_PATH = "perf_report.json"
DEFAULT_RAW_STATS_DIR = "."

# Auto-compact / compact options
DEFAULT_RUN_COMPACT = False  # run db.collection.compact() after delete
DEFAULT_AUTO_COMPACT = False  # enable MongoDB 8 autoCompact server param

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def fmt_bytes(n: int) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} PB"


def now_ts() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def random_hex(length: int = 32) -> str:
    return "".join(random.choices(string.hexdigits[:16], k=length))


def make_document() -> dict:
    now = datetime.datetime.utcnow()
    delta = datetime.timedelta(seconds=random.randint(0, 86_400 * 365))
    return {
        "hashField": random_hex(32),
        "createdAt": now - delta,
        "updatedAt": now,
        "customerId": str(uuid.uuid4()),
        "x": 1,
        "y": 2,
        "z": 3,
        "status": random.choice(["active", "inactive", "pending"]),
        "region": random.choice(["EU", "US", "APAC", "LATAM"]),
        "priority": random.randint(1, 10),
    }


def get_raw_collection_stats(db, coll_name: str) -> Dict[str, Any]:
    """Return the full raw output of db.command('collStats', coll_name)."""
    return db.command("collStats", coll_name)


def summarize_collection_stats(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull summarized storage statistics from raw collStats output.
    Returns a dict with human-readable and raw byte values.
    """
    wt = raw.get("wiredTiger", {})
    bm = wt.get("block-manager", {})

    stats = {
        "timestamp": now_ts(),
        "count": raw.get("count", 0),

        # Logical document size (uncompressed)
        "size_bytes": raw.get("size", 0),
        "size_human": fmt_bytes(raw.get("size", 0)),

        # Physical storage allocated on disk (compressed)
        "storageSize_bytes": raw.get("storageSize", 0),
        "storageSize_human": fmt_bytes(raw.get("storageSize", 0)),

        # Index storage
        "totalIndexSize_bytes": raw.get("totalIndexSize", 0),
        "totalIndexSize_human": fmt_bytes(raw.get("totalIndexSize", 0)),

        # Total on-disk footprint (data + indexes)
        "totalSize_bytes": raw.get("totalSize", 0),
        "totalSize_human": fmt_bytes(raw.get("totalSize", 0)),

        # WiredTiger internals – bytes reusable (freed but not yet returned to OS)
        "wt_bytes_available_reuse": bm.get("file bytes available for reuse", 0),
        "wt_bytes_available_reuse_human": fmt_bytes(
            bm.get("file bytes available for reuse", 0)
        ),

        # WiredTiger – total file size on disk
        "wt_file_size_bytes": bm.get("file size in bytes", 0),
        "wt_file_size_human": fmt_bytes(bm.get("file size in bytes", 0)),
    }
    return stats


def get_collection_stats(db, coll_name: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Return both raw collStats output and summarized stats."""
    raw = get_raw_collection_stats(db, coll_name)
    summary = summarize_collection_stats(raw)
    return raw, summary


def save_raw_stats(raw: Dict[str, Any], output_dir: str, label: str) -> Dict[str, str]:
    """
    Save raw collStats output in both JSON and TXT formats.
    Returns the written file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base_name = f"collstats_{label}_{timestamp}"

    json_path = os.path.join(output_dir, f"{base_name}.json")
    txt_path = os.path.join(output_dir, f"{base_name}.txt")

    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(raw, jf, indent=2, default=str)

    with open(txt_path, "w", encoding="utf-8") as tf:
        tf.write(pprint.pformat(raw, width=120, sort_dicts=False))
        tf.write("\n")

    return {"json": json_path, "txt": txt_path}


def print_stats(label: str, stats: Dict[str, Any]) -> None:
    print(f"\n{'─' * 60}")
    print(f" 📊 {label} [{stats['timestamp']}]")
    print(f"{'─' * 60}")
    print(f" Document count           : {stats['count']:>15,}")
    print(f" Logical size (docs)      : {stats['size_human']:>15}")
    print(f" Storage size (on disk)   : {stats['storageSize_human']:>15} ← primary metric")
    print(f" Total index size         : {stats['totalIndexSize_human']:>15}")
    print(f" Total size (data+idx)    : {stats['totalSize_human']:>15}")
    print(f" WT file size             : {stats['wt_file_size_human']:>15}")
    print(f" WT reusable bytes        : {stats['wt_bytes_available_reuse_human']:>15}")
    print(f"{'─' * 60}")


def countdown(seconds: int) -> None:
    print()
    for remaining in range(seconds, 0, -10):
        print(f" ⏱ Waiting … {remaining:>4}s remaining", end="\r", flush=True)
        time.sleep(min(10, remaining))
    print(" " * 50, end="\r")  # clear line


def insert_documents(collection, total: int, batch_size: int) -> float:
    """Batch-insert `total` documents. Returns elapsed seconds."""
    inserted = 0
    start_time = time.perf_counter()

    while inserted < total:
        this_batch = min(batch_size, total - inserted)
        docs = [make_document() for _ in range(this_batch)]
        collection.insert_many(docs, ordered=False)
        inserted += this_batch
        elapsed = time.perf_counter() - start_time
        rate = inserted / elapsed if elapsed > 0 else 0
        pct = inserted / total * 100
        remaining_s = (total - inserted) / rate if rate > 0 else 0
        print(
            f" ✏ Inserted {inserted:>12,} / {total:,} "
            f"({pct:5.1f}%) "
            f"{rate:,.0f} docs/s "
            f"ETA {remaining_s:.0f}s ",
            end="\r",
            flush=True,
        )

    elapsed = time.perf_counter() - start_time
    print(f"\n\n ✅ Inserted {inserted:,} docs in {elapsed:.1f}s ({inserted / elapsed:,.0f} docs/s)")
    return elapsed


def delete_documents(collection) -> tuple:
    """Delete all documents. Returns (deleted_count, elapsed_seconds)."""
    start = time.perf_counter()
    result = collection.delete_many({})
    elapsed = time.perf_counter() - start
    print(
        f"\n 🗑 Deleted {result.deleted_count:,} docs in {elapsed:.1f}s "
        f"({result.deleted_count / elapsed:,.0f} docs/s)"
    )
    return result.deleted_count, elapsed


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MongoDB V8 Bulk Delete Performance Test")
    p.add_argument("--uri", default=DEFAULT_URI, help="MongoDB URI")
    p.add_argument("--db", default=DEFAULT_DB, help="Database name")
    p.add_argument("--collection", default=DEFAULT_COLLECTION, help="Collection name")
    p.add_argument(
        "--total-docs",
        default=DEFAULT_TOTAL_DOCS,
        type=int,
        help=f"Total documents to insert (default: {DEFAULT_TOTAL_DOCS:,})",
    )
    p.add_argument(
        "--batch-size",
        default=DEFAULT_BATCH_SIZE,
        type=int,
        help=f"insertMany batch size (default: {DEFAULT_BATCH_SIZE:,})",
    )
    p.add_argument(
        "--wait",
        default=DEFAULT_WAIT_SECONDS,
        type=int,
        help=f"Seconds to wait before delete (default: {DEFAULT_WAIT_SECONDS})",
    )
    p.add_argument(
        "--run-compact",
        action="store_true",
        default=DEFAULT_RUN_COMPACT,
        help="Run compact() after delete and capture stats",
    )
    p.add_argument(
        "--auto-compact",
        action="store_true",
        default=DEFAULT_AUTO_COMPACT,
        help="Enable MongoDB 8 autoCompact server parameter before insert",
    )
    p.add_argument(
        "--report",
        default=DEFAULT_REPORT_PATH,
        help=f"Output JSON report path (default: {DEFAULT_REPORT_PATH})",
    )
    p.add_argument(
        "--raw-stats-dir",
        default=DEFAULT_RAW_STATS_DIR,
        help="Directory where raw collStats JSON/TXT files will be saved",
    )
    p.add_argument("--drop-first", action="store_true", help="Drop existing collection before test")
    p.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip index creation (useful for raw insert baseline)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print("\n" + "═" * 60)
    print(" MongoDB V8 · Bulk Delete Performance Test")
    print("═" * 60)
    print(f" URI          : {args.uri}")
    print(f" Database     : {args.db}")
    print(f" Collection   : {args.collection}")
    print(f" Total docs   : {args.total_docs:,}")
    print(f" Batch size   : {args.batch_size:,}")
    print(f" Wait time    : {args.wait}s")
    print(f" Run compact  : {args.run_compact}")
    print(f" Auto compact : {args.auto_compact}")
    print(f" Raw stats dir: {args.raw_stats_dir}")
    print("═" * 60)

    report: Dict[str, Any] = {"test_start": now_ts(), "config": vars(args), "phases": {}}

    print(f"\n[1/7] Connecting to MongoDB …")
    client = MongoClient(args.uri, serverSelectionTimeoutMS=5_000)
    try:
        info = client.admin.command("serverStatus")
        version = info.get("version", "unknown")
        print(f" ✅ Connected · MongoDB {version}")
        report["mongodb_version"] = version
    except Exception as exc:
        print(f" ❌ Cannot connect: {exc}")
        sys.exit(1)

    db = client[args.db]
    coll = db[args.collection]

    if args.auto_compact:
        print("\n[*] Enabling autoCompact server parameter …")
        try:
            client.admin.command({"setParameter": 1, "autoCompact": True})
            print(" ✅ autoCompact = true")
        except Exception as exc:
            print(f" ⚠ Could not set autoCompact: {exc}")

    if args.drop_first:
        print(f"\n[*] Dropping existing collection '{args.collection}' …")
        coll.drop()
        print(" ✅ Dropped.")

    if not args.skip_indexes:
        print(f"\n[2/7] Creating indexes …")
        coll.create_index([("hashField", pymongo.HASHED)], name="hashField_hashed")
        coll.create_index([("createdAt", pymongo.ASCENDING)], name="createdAt_1")
        coll.create_index([("customerId", pymongo.ASCENDING)], name="customerId_1")
        print(" ✅ Indexes created: hashField (hashed), createdAt, customerId")
    else:
        print("\n[2/7] Skipping index creation (--skip-indexes).")

    print(f"\n[3/7] Inserting {args.total_docs:,} documents …")
    insert_elapsed = insert_documents(coll, args.total_docs, args.batch_size)
    report["phases"]["insert"] = {"elapsed_seconds": round(insert_elapsed, 2)}

    print("\n[4/7] Capturing storage stats AFTER INSERT …")
    raw_after_insert, stats_after_insert = get_collection_stats(db, args.collection)
    print_stats("AFTER INSERT", stats_after_insert)
    report["phases"]["after_insert"] = stats_after_insert

    raw_insert_paths = save_raw_stats(raw_after_insert, args.raw_stats_dir, "after_insert")
    report["phases"]["after_insert_raw_files"] = raw_insert_paths
    print(f" 💾 Raw AFTER INSERT stats saved to:")
    print(f"    JSON: {raw_insert_paths['json']}")
    print(f"    TXT : {raw_insert_paths['txt']}")

    print(f"\n[5/7] Waiting {args.wait}s before delete …")
    countdown(args.wait)
    print(" ✅ Wait complete.")

    print(f"\n[6/7] Deleting all {args.total_docs:,} documents …")
    deleted_count, delete_elapsed = delete_documents(coll)
    report["phases"]["delete"] = {
        "deleted_count": deleted_count,
        "elapsed_seconds": round(delete_elapsed, 2),
    }

    print("\n[7/7] Capturing storage stats AFTER DELETE …")
    raw_after_delete, stats_after_delete = get_collection_stats(db, args.collection)
    print_stats("AFTER DELETE (immediate)", stats_after_delete)
    report["phases"]["after_delete_immediate"] = stats_after_delete

    raw_delete_paths = save_raw_stats(raw_after_delete, args.raw_stats_dir, "after_delete")
    report["phases"]["after_delete_raw_files"] = raw_delete_paths
    print(f" 💾 Raw AFTER DELETE stats saved to:")
    print(f"    JSON: {raw_delete_paths['json']}")
    print(f"    TXT : {raw_delete_paths['txt']}")

    if args.run_compact:
        print("\n[+] Running compact() — this may take several minutes …")
        compact_start = time.perf_counter()
        try:
            db.command({"compact": args.collection})
            compact_elapsed = time.perf_counter() - compact_start
            print(f" ✅ compact() finished in {compact_elapsed:.1f}s")
            _, stats_after_compact = get_collection_stats(db, args.collection)
            print_stats("AFTER COMPACT", stats_after_compact)
            report["phases"]["compact_elapsed_seconds"] = round(compact_elapsed, 2)
            report["phases"]["after_compact"] = stats_after_compact
        except Exception as exc:
            print(f" ⚠ compact() failed: {exc}")

    ins = report["phases"].get("after_insert", {})
    dlt = report["phases"].get("after_delete_immediate", {})
    cpt = report["phases"].get("after_compact", {})

    print("\n" + "═" * 70)
    print(" SUMMARY — Disk Space Comparison")
    print("═" * 70)
    header = f" {'Metric':<35} {'After Insert':>15} {'After Delete':>15}"
    if cpt:
        header += f" {'After Compact':>15}"
    print(header)
    print(" " + "─" * 67)

    metrics = [
        ("storageSize (data, on disk)", "storageSize_human"),
        ("totalIndexSize", "totalIndexSize_human"),
        ("Total size (data+idx)", "totalSize_human"),
        ("WT file size", "wt_file_size_human"),
        ("WT reusable bytes", "wt_bytes_available_reuse_human"),
        ("Document count", "count"),
    ]
    for label, key in metrics:
        vi = ins.get(key, "—")
        vd = dlt.get(key, "—")
        if isinstance(vi, int):
            vi = f"{vi:,}"
        if isinstance(vd, int):
            vd = f"{vd:,}"
        row = f" {label:<35} {str(vi):>15} {str(vd):>15}"
        if cpt:
            vc = cpt.get(key, "—")
            if isinstance(vc, int):
                vc = f"{vc:,}"
            row += f" {str(vc):>15}"
        print(row)

    print(" " + "─" * 67)
    if ins.get("storageSize_bytes") and dlt.get("storageSize_bytes"):
        freed = ins["storageSize_bytes"] - dlt["storageSize_bytes"]
        freed_pct = freed / ins["storageSize_bytes"] * 100 if ins["storageSize_bytes"] else 0
        row = f" {'Storage freed after delete':<35} {fmt_bytes(freed):>15} ({freed_pct:.1f}%)"
        print(row)
        if cpt and ins.get("storageSize_bytes") and cpt.get("storageSize_bytes"):
            freed_c = ins["storageSize_bytes"] - cpt["storageSize_bytes"]
            freed_pct_c = freed_c / ins["storageSize_bytes"] * 100 if ins["storageSize_bytes"] else 0
            row = f" {'Storage freed after compact':<35} {fmt_bytes(freed_c):>15} ({freed_pct_c:.1f}%)"
            print(row)

    print("═" * 70)

    print(
        """
 📌 INITIAL SYNC COMPARISON TIPS
 ─────────────────────────────────────────────────────────────────────
 • Add a secondary member BEFORE the test starts to measure sync
 time over the full 10M document insert.
 • Add a secondary member AFTER the delete to see how much data
 the secondary needs to clone — a much smaller dataset when
 autoCompact has released pages.
 • Key commands for initial sync monitoring on the secondary:
 rs.status() → syncSourceHost, state
 db.adminCommand({replSetGetStatus:1}) → initialSyncStatus
 • Key commands to watch WiredTiger reclaim progress over time:
 db.{COLL}.stats().wiredTiger["block-manager"]
 → "file bytes available for reuse" (shrinks as compaction runs)
 → "file size in bytes" (drops after OS-level release)
 • To enable autoCompact on MongoDB 8 (background compaction):
 db.adminCommand({{ setParameter: 1, autoCompact: true }})
 • To trigger foreground compaction manually:
 db.runCommand({{ compact: "{COLL}" }})
 ─────────────────────────────────────────────────────────────────────
""".format(COLL=args.collection)
    )

    report["test_end"] = now_ts()
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f" 💾 Report saved to: {args.report}\n")

    client.close()


if __name__ == "__main__":
    main()
