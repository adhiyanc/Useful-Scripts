import json
from pathlib import Path
from datetime import datetime, timezone

FILE = Path("/Users/adhiyan.chattopadhyay/Documents/decoded_ftdc.json")

def num(v):
    if isinstance(v, dict):
        for k in ("$numberLong", "$numberInt", "$numberDouble"):
            if k in v:
                return int(float(v[k])) if k == "$numberDouble" else int(v[k])
    return v

def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")

def parse_snapshots(path: Path):
    """
    Parses concatenated JSON arrays/objects and stops cleanly if the file
    ends with an incomplete/truncated JSON block.
    """
    text = path.read_text(encoding="iso-8859-1")
    dec = json.JSONDecoder()
    idx = 0
    snapshots = []

    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break

        try:
            obj, end = dec.raw_decode(text, idx)
        except json.JSONDecodeError:
            break

        idx = end
        if isinstance(obj, list):
            snapshots.extend(obj)
        else:
            snapshots.append(obj)

    return snapshots

def fmt_duration(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds - hours * 3600 - minutes * 60
    return f"{hours}h {minutes}m {secs:.3f}s"

def summarize(path: Path):
    snapshots = parse_snapshots(path)
    if not snapshots:
        raise RuntimeError("No complete snapshots could be parsed")

    first = snapshots[0]
    last = snapshots[-1]
    counters = ["insert", "query", "update", "delete", "getmore", "command"]

    first_local_ms = num(first["serverStatus"]["localTime"]["$date"])
    last_local_ms = num(last["serverStatus"]["localTime"]["$date"])

    first_ops = {k: num(first["serverStatus"]["opcounters"][k]) for k in counters}
    last_ops = {k: num(last["serverStatus"]["opcounters"][k]) for k in counters}
    delta_ops = {k: last_ops[k] - first_ops[k] for k in counters}

    intervals = []
    prev = None
    for s in snapshots:
        cur = num(s["serverStatus"]["localTime"]["$date"])
        if prev is not None:
            intervals.append(cur - prev)
        prev = cur

    return {
        "snapshots": len(snapshots),
        "start_timestamp": ms_to_iso(first_local_ms),
        "end_timestamp": ms_to_iso(last_local_ms),
        "elapsed_seconds": (last_local_ms - first_local_ms) / 1000,
        "avg_interval_seconds": (sum(intervals) / len(intervals) / 1000) if intervals else 0,
        "start_opcounters": first_ops,
        "end_opcounters": last_ops,
        "delta_opcounters": delta_ops,
        "start_total_ops": sum(first_ops.values()),
        "end_total_ops": sum(last_ops.values()),
        "delta_total_ops": sum(delta_ops.values()),
        "first_top_start": ms_to_iso(num(first["start"]["$date"])),
        "first_top_end": ms_to_iso(num(first["end"]["$date"])),
        "last_top_start": ms_to_iso(num(last["start"]["$date"])),
        "last_top_end": ms_to_iso(num(last["end"]["$date"])),
    }

def print_report(summary):
    print("# JSON Metrics Timeframe Report\n")

    print("## Summary\n")
    print("| Metric | Value |")
    print("| --- | --- |")
    print(f"| Complete snapshots parsed | {summary['snapshots']} |")
    print(f"| Start timestamp | {summary['start_timestamp']} |")
    print(f"| End timestamp | {summary['end_timestamp']} |")
    print(f"| Elapsed time | {fmt_duration(summary['elapsed_seconds'])} |")
    print(f"| Average sample interval | {summary['avg_interval_seconds']:.3f}s |")

    print("\n## Operation counts at start and end\n")
    print("| Counter | Start | End | Delta |")
    print("| --- | ---: | ---: | ---: |")
    for k in ["insert", "query", "update", "delete", "getmore", "command"]:
        print(f"| {k} | {summary['start_opcounters'][k]} | {summary['end_opcounters'][k]} | {summary['delta_opcounters'][k]} |")
    print(f"| total | {summary['start_total_ops']} | {summary['end_total_ops']} | {summary['delta_total_ops']} |")

    print("\n## Snapshot envelope details\n")
    print("| Boundary | Top-level start | serverStatus.localTime | Top-level end |")
    print("| --- | --- | --- | --- |")
    print(f"| First complete snapshot | {summary['first_top_start']} | {summary['start_timestamp']} | {summary['first_top_end']} |")
    print(f"| Last complete snapshot | {summary['last_top_start']} | {summary['end_timestamp']} | {summary['last_top_end']} |")

if __name__ == "__main__":
    summary = summarize(FILE)
    print_report(summary)
