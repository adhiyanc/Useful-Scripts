#!/usr/bin/env python3
import os
import re
import zlib
import json
import argparse
import datetime
import struct

MASK = (1 << 64) - 1
EPOCH = datetime.datetime(1970, 1, 1)
FILE_RE = re.compile(r"^metrics\.(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})Z-\d+$")
TARGET_METRICS = [
    "opcounters.command",
    "opcounters.delete",
    "opcounters.getmore",
    "opcounters.insert",
    "opcounters.query",
    "opcounters.update",
]

def to_ms(dt):
    return int((dt - EPOCH).total_seconds() * 1000)

def parse_time(value):
    value = value.strip()

    if value.isdigit():
        return int(value)

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        dt = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        else:
            raise argparse.ArgumentTypeError(
                "invalid time %r; use ISO-8601, YYYY-MM-DD HH:MM[:SS], or epoch milliseconds"
                % value
            )

    if dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    return to_ms(dt)

def ms_to_iso_z(ms):
    dt = datetime.datetime.fromtimestamp(ms / 1000.0, datetime.timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _read_cstring(buf, pos):
    end = buf.index(b"\x00", pos)
    return buf[pos:end].decode("utf-8", "replace"), end + 1

def parse_document(buf, pos):
    doc_len = struct.unpack_from("<i", buf, pos)[0]
    end = pos + doc_len
    pos += 4
    items = []

    while pos < end - 1:
        tag = buf[pos]
        pos += 1
        key, pos = _read_cstring(buf, pos)

        if tag == 0x01:
            value = struct.unpack_from("<d", buf, pos)[0]
            pos += 8
            items.append((key, "double", value))
        elif tag in (0x02, 0x0D, 0x0E):
            slen = struct.unpack_from("<i", buf, pos)[0]
            pos += 4
            value = buf[pos:pos + slen - 1].decode("utf-8", "replace")
            pos += slen
            items.append((key, "string", value))
        elif tag == 0x03:
            sub, pos = parse_document(buf, pos)
            items.append((key, "doc", sub))
        elif tag == 0x04:
            sub, pos = parse_document(buf, pos)
            items.append((key, "arr", sub))
        elif tag == 0x05:
            blen = struct.unpack_from("<i", buf, pos)[0]
            pos += 4
            subtype = buf[pos]
            pos += 1
            data = buf[pos:pos + blen]
            pos += blen
            items.append((key, "bin", (subtype, data)))
        elif tag == 0x06:
            items.append((key, "undef", None))
        elif tag == 0x07:
            oid = buf[pos:pos + 12]
            pos += 12
            items.append((key, "oid", oid))
        elif tag == 0x08:
            value = bool(buf[pos])
            pos += 1
            items.append((key, "bool", value))
        elif tag == 0x09:
            value = struct.unpack_from("<q", buf, pos)[0]
            pos += 8
            items.append((key, "datetime", value))
        elif tag == 0x0A:
            items.append((key, "null", None))
        elif tag == 0x0B:
            _, pos = _read_cstring(buf, pos)
            _, pos = _read_cstring(buf, pos)
            items.append((key, "regex", None))
        elif tag == 0x0C:
            slen = struct.unpack_from("<i", buf, pos)[0]
            pos += 4
            pos += slen
            pos += 12
            items.append((key, "dbptr", None))
        elif tag == 0x0F:
            total = struct.unpack_from("<i", buf, pos)[0]
            pos += total
            items.append((key, "codews", None))
        elif tag == 0x10:
            value = struct.unpack_from("<i", buf, pos)[0]
            pos += 4
            items.append((key, "int32", value))
        elif tag == 0x11:
            inc = struct.unpack_from("<I", buf, pos)[0]
            secs = struct.unpack_from("<I", buf, pos + 4)[0]
            pos += 8
            items.append((key, "timestamp", (secs, inc)))
        elif tag == 0x12:
            value = struct.unpack_from("<q", buf, pos)[0]
            pos += 8
            items.append((key, "int64", value))
        elif tag == 0x13:
            pos += 16
            items.append((key, "dec128", None))
        elif tag == 0xFF:
            items.append((key, "minkey", None))
        elif tag == 0x7F:
            items.append((key, "maxkey", None))
        else:
            raise ValueError("unknown BSON type 0x%02x for key %r" % (tag, key))

    return items, end

def iter_top_level_docs(buf):
    pos = 0
    total = len(buf)
    while pos + 4 <= total:
        doc_len = struct.unpack_from("<i", buf, pos)[0]
        if doc_len <= 0 or pos + doc_len > total:
            break
        items, _ = parse_document(buf, pos)
        yield items
        pos += doc_len

def flatten_metric_items(items, prefix, out):
    for key, tag, value in items:
        path = key if not prefix else prefix + "." + key

        if tag in ("doc", "arr"):
            flatten_metric_items(value, path, out)
        elif tag == "double":
            out.append((path, int(value) & MASK))
        elif tag in ("int32", "int64"):
            out.append((path, int(value) & MASK))
        elif tag == "bool":
            out.append((path, 1 if value else 0))
        elif tag == "datetime":
            out.append((path, int(value) & MASK))
        elif tag == "timestamp":
            secs, inc = value
            out.append((path, secs & MASK))
            out.append((path + ".inc", inc & MASK))

def find_field(items, key):
    for item_key, tag, value in items:
        if item_key == key:
            return tag, value
    return None, None

def uvarint(buf, pos):
    result = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos

def decode_chunk(raw):
    payload = zlib.decompress(raw[4:])
    ref_len = struct.unpack_from("<i", payload, 0)[0]
    ref_items, _ = parse_document(payload, 0)

    ref_metrics = []
    flatten_metric_items(ref_items, "", ref_metrics)

    names = [name for name, _ in ref_metrics]
    values = [[value] for _, value in ref_metrics]

    pos = ref_len
    metric_count = struct.unpack_from("<I", payload, pos)[0]
    pos += 4
    sample_count = struct.unpack_from("<I", payload, pos)[0]
    pos += 4

    if len(names) != metric_count:
        raise ValueError(
            "metric count mismatch: extracted %d but header says %d" % (len(names), metric_count)
        )

    total = metric_count * sample_count
    deltas = []
    while len(deltas) < total:
        delta, pos = uvarint(payload, pos)
        deltas.append(delta)
        if delta == 0:
            zero_count, pos = uvarint(payload, pos)
            deltas.extend([0] * zero_count)

    if len(deltas) > total:
        deltas = deltas[:total]

    for metric_index in range(metric_count):
        current = values[metric_index][0]
        base = metric_index * sample_count
        for sample_index in range(sample_count):
            current = (current + deltas[base + sample_index]) & MASK
            values[metric_index].append(current)

    return names, values

def parse_file_start_ms(filename):
    match = FILE_RE.match(filename)
    if not match:
        return None
    dt = datetime.datetime.strptime(match.group(1), "%Y-%m-%dT%H-%M-%S")
    return to_ms(dt)

def list_metric_files(metrics_dir):
    files = []
    for name in os.listdir(metrics_dir):
        start_ms = parse_file_start_ms(name)
        if start_ms is None:
            continue
        path = os.path.join(metrics_dir, name)
        if os.path.isfile(path):
            files.append((start_ms, path, name))
    files.sort(key=lambda x: (x[0], x[2]))
    return files

def select_relevant_files(files, start_ms, end_ms):
    start_idx = None
    end_idx = None

    for i, item in enumerate(files):
        file_start_ms = item[0]
        if start_ms <= file_start_ms <= end_ms:
            start_idx = i
            break

    if start_idx is None:
        for i, item in enumerate(files):
            file_start_ms = item[0]
            if file_start_ms <= start_ms:
                start_idx = i
            else:
                break

    for i, item in enumerate(files):
        file_start_ms = item[0]
        if file_start_ms <= end_ms:
            end_idx = i
        else:
            break

    if start_idx is None:
        raise ValueError("no metrics file is usable for the requested start time")
    if end_idx is None:
        raise ValueError("no metrics file begins on or before the requested end time")
    if end_idx < start_idx:
        raise ValueError("requested end time falls before the selected start file")

    return files[start_idx:end_idx + 1]

def target_metric_name(full_name):
    for target in TARGET_METRICS:
        if full_name == target or full_name.endswith("." + target):
            return target
    return None

def update_stat(stat, sample_ts, sample_value):
    if stat["first_ts"] is None or sample_ts < stat["first_ts"]:
        stat["first_ts"] = sample_ts
        stat["first_value"] = sample_value
    if stat["last_ts"] is None or sample_ts >= stat["last_ts"]:
        stat["last_ts"] = sample_ts
        stat["last_value"] = sample_value

def gather_stats(selected_files, start_ms, end_ms):
    stats = {}
    for metric in TARGET_METRICS:
        stats[metric] = {
            "first_ts": None,
            "first_value": None,
            "last_ts": None,
            "last_value": None,
        }

    matched_samples = 0

    for _, path, _ in selected_files:
        with open(path, "rb") as fh:
            data = fh.read()

        for items in iter_top_level_docs(data):
            tag, type_value = find_field(items, "type")
            if type_value != 1:
                continue

            bin_tag, bin_value = find_field(items, "data")
            if bin_tag != "bin" or bin_value is None:
                continue

            _, chunk_data = bin_value
            names, values = decode_chunk(chunk_data)

            try:
                ts_idx = names.index("start")
            except ValueError:
                continue

            chunk_ts = values[ts_idx]

            for sample_idx, sample_ts in enumerate(chunk_ts):
                if not (start_ms <= sample_ts <= end_ms):
                    continue

                matched_samples += 1

                for name, metric_values in zip(names, values):
                    target = target_metric_name(name)
                    if target is None:
                        continue
                    update_stat(stats[target], sample_ts, metric_values[sample_idx])

    return stats, matched_samples

def build_output(shard, start_ms, end_ms, stats, matched_samples, selected_files):
    output = {
        "shard": str(shard),
        "requested_start": ms_to_iso_z(start_ms),
        "requested_end": ms_to_iso_z(end_ms),
        "interval_seconds": int((end_ms - start_ms) / 1000),
        "selected_metrics_files": [
            {
                "name": name,
                "path": path,
                "file_start": ms_to_iso_z(file_start_ms),
            }
            for file_start_ms, path, name in selected_files
        ],
        "matched_samples": matched_samples,
        "metrics": {},
    }

    for metric in TARGET_METRICS:
        stat = stats[metric]
        if stat["first_ts"] is None:
            output["metrics"][metric] = None
            continue

        output["metrics"][metric] = {
            "first_sample_time": ms_to_iso_z(stat["first_ts"]),
            "last_sample_time": ms_to_iso_z(stat["last_ts"]),
            "first_value": stat["first_value"],
            "last_value": stat["last_value"],
            "delta": stat["last_value"] - stat["first_value"],
        }

    if matched_samples == 0:
        output["message"] = "No samples matched the requested time range."
    elif all(output["metrics"][metric] is None for metric in TARGET_METRICS):
        output["message"] = "Samples matched the requested time range, but no requested metrics were found."

    return output

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", required=True, help="shard number/label to print")
    parser.add_argument("--dir", dest="metrics_dir", required=True, help="directory containing metrics.* files")
    parser.add_argument("--start", required=True, type=parse_time, help="inclusive start time")
    parser.add_argument("--end", required=True, type=parse_time, help="inclusive end time")
    args = parser.parse_args()

    if args.end < args.start:
        print(json.dumps({"error": "--end must be greater than or equal to --start"}, indent=2))
        return 2

    files = list_metric_files(args.metrics_dir)
    if not files:
        print(json.dumps({"error": "No metrics files found in %s" % args.metrics_dir}, indent=2))
        return 2

    try:
        selected_files = select_relevant_files(files, args.start, args.end)
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}, indent=2))
        return 2

    try:
        stats, matched_samples = gather_stats(selected_files, args.start, args.end)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, indent=2))
        return 2

    output = build_output(args.shard, args.start, args.end, stats, matched_samples, selected_files)
    print(json.dumps(output, indent=2, sort_keys=False))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
