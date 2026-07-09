#!/usr/bin/env python3
import os
import ast
import csv
import json
import shlex
import posixpath
import argparse
import datetime
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

DECODE_SCRIPT = Path("./decode_helper_py36.py")
OUTPUT_DIR = Path("./shard_outputs")

TARGETS = ast.literal_eval(os.environ.get("PRIMARY_TARGETS", "[]"))

CSV_COLUMNS = [
    "shard",
    "metric",
    "start",
    "end",
    "first_value",
    "last_value",
    "seconds",
    "delta",
]

def validate_time_arg(value):
    value = value.strip()

    if value.isdigit():
        return value

    if value.endswith("Z"):
        try:
            datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
            return value
        except ValueError:
            pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            datetime.datetime.strptime(value, fmt)
            return value
        except ValueError:
            continue

    raise argparse.ArgumentTypeError(
        "invalid time %r; use ISO-8601, YYYY-MM-DD HH:MM[:SS], or epoch milliseconds"
        % value
    )

def time_to_ms_for_validation(value):
    value = value.strip()

    if value.isdigit():
        return int(value)

    if value.endswith("Z"):
        dt = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    else:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError("invalid time %r" % value)

    epoch = datetime.datetime(1970, 1, 1)
    return int((dt - epoch).total_seconds() * 1000)

def validate_dir_prefix(value):
    value = value.strip()
    if not value:
        raise argparse.ArgumentTypeError("--dir-prefix cannot be empty")
    if not value.startswith("/"):
        raise argparse.ArgumentTypeError("--dir-prefix must be an absolute path")
    return value.rstrip("/")

def build_metrics_dir(dir_prefix, shard):
    return posixpath.join(dir_prefix, str(shard), "data", "diagnostic.data")

def run_remote_decode(shard, host, script_text, start, end, dir_prefix):
    metrics_dir = build_metrics_dir(dir_prefix, shard)

    remote_python_cmd = (
        "python3 - "
        f"--shard {shlex.quote(str(shard))} "
        f"--dir {shlex.quote(metrics_dir)} "
        f"--start {shlex.quote(start)} "
        f"--end {shlex.quote(end)}"
    )

    ssh_cmd = [
        "ssh",
        f"{host}",
        remote_python_cmd,
    ]

    proc = subprocess.run(
        ssh_cmd,
        input=script_text,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    result = {
        "shard": str(shard),
        "host": host,
        "metrics_dir": metrics_dir,
        "returncode": proc.returncode,
    }

    if proc.returncode == 0:
        try:
            result["data"] = json.loads(proc.stdout)
        except json.JSONDecodeError:
            result["error"] = "remote stdout was not valid JSON"
            result["stdout"] = proc.stdout
            result["stderr"] = proc.stderr
    else:
        result["error"] = "ssh/remote command failed"
        result["stderr"] = proc.stderr
        result["stdout"] = proc.stdout

    return result

def run_local_decode(shard, dir_prefix, start, end, script_path=DECODE_SCRIPT):
    metrics_dir = build_metrics_dir(dir_prefix, shard)

    local_cmd = [
        "python3",
        str(script_path),
        "--shard",
        str(shard),
        "--dir",
        metrics_dir,
        "--start",
        start,
        "--end",
        end,
    ]

    proc = subprocess.run(
        local_cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    result = {
        "shard": str(shard),
        "host": "local",
        "metrics_dir": metrics_dir,
        "returncode": proc.returncode,
    }

    if proc.returncode == 0:
        try:
            result["data"] = json.loads(proc.stdout)
        except json.JSONDecodeError:
            result["error"] = "local stdout was not valid JSON"
            result["stdout"] = proc.stdout
            result["stderr"] = proc.stderr
    else:
        result["error"] = "local command failed"
        result["stdout"] = proc.stdout
        result["stderr"] = proc.stderr

    return result

def save_shard_result(output_dir, result):
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / ('%s.json' % result["shard"])
    out_path.write_text(json.dumps(result, indent=2))
    return out_path

def build_csv_rows(results):
    rows = []

    for result in results:
        data = result.get("data")
        if not data:
            continue

        shard = data.get("shard", result["shard"])
        start = data.get("requested_start")
        end = data.get("requested_end")
        seconds = data.get("interval_seconds")
        metrics = data.get("metrics", {})

        for metric, metric_data in metrics.items():
            if metric_data is None:
                continue

            rows.append({
                "shard": shard,
                "metric": metric,
                "start": start,
                "end": end,
                "first_value": metric_data.get("first_value"),
                "last_value": metric_data.get("last_value"),
                "seconds": seconds,
                "delta": metric_data.get("delta"),
            })

    return rows

def write_csv(output_path, rows):
    with output_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        required=True,
        type=validate_time_arg,
        help="inclusive start time",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=validate_time_arg,
        help="inclusive end time",
    )
    parser.add_argument(
        "--dir-prefix",
        required=True,
        type=validate_dir_prefix,
        help="prefix such as /d/d3/sem2/qa; script appends {shard}/data/diagnostic.data",
    )
    args = parser.parse_args()

    if time_to_ms_for_validation(args.end) < time_to_ms_for_validation(args.start):
        parser.error("--end must be greater than or equal to --start")

    script_text = DECODE_SCRIPT.read_text()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    with ThreadPoolExecutor(max_workers=len(TARGETS) or 1) as executor:
        future_to_target = {
            executor.submit(
                run_remote_decode,
                shard,
                host,
                script_text,
                args.start,
                args.end,
                args.dir_prefix,
            ): (shard, host)
            # executor.submit(
            #     run_local_decode,
            #     shard,
            #     args.dir_prefix,
            #     args.start,
            #     args.end,
            #     script_path=DECODE_SCRIPT,
            # ): (shard, host)
            for shard, host in TARGETS
        }

        for future in as_completed(future_to_target):
            shard, host = future_to_target[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "shard": str(shard),
                    "host": host,
                    "metrics_dir": build_metrics_dir(args.dir_prefix, shard),
                    "returncode": -1,
                    "error": "parallel worker failed: %s" % exc,
                }

            save_shard_result(OUTPUT_DIR, result)
            results.append(result)

    results.sort(key=lambda r: r["shard"])

    combined_path = OUTPUT_DIR / "all_shards.json"
    combined_path.write_text(json.dumps(results, indent=2))

    csv_rows = build_csv_rows(results)
    csv_path = OUTPUT_DIR / "all_shards.csv"
    write_csv(csv_path, csv_rows)

    print(json.dumps({
        "all_shards_json": str(combined_path),
        "all_shards_csv": str(csv_path),
        "rows_written": len(csv_rows),
        "results": results,
    }, indent=2))

if __name__ == "__main__":
    main()
