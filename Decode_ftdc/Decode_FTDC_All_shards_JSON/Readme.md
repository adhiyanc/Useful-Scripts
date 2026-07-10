# Shard Metrics Toolkit

## Overview

This toolkit is made of two scripts that work together:

1. **Shard collection orchestrator**  
   Runs from a central machine, fans out across shard hosts over SSH, and aggregates results.

2. **FTDC decode helper**  
   Runs on each target host, reads MongoDB FTDC `metrics.*` files, decodes them, and returns a JSON summary for selected metrics.

Together, they let you collect shard-level operation counter deltas across a time window and export the results as both JSON and CSV.

---

## Architecture

### Script 1: Orchestrator ( a.k.a - decode_shardv4.py ) 

The orchestrator is responsible for:

- reading shard/host pairs from `PRIMARY_TARGETS`
- validating `--start`, `--end`, and `--dir-prefix`
- building the remote `diagnostic.data` path for each shard
- streaming the helper script to each host over SSH
- collecting the returned JSON
- writing per-shard and combined output files

### Script 2: Decode helper ( a.k.a decode_helper_py36.py )

The helper is responsible for:

- scanning a single `diagnostic.data` directory
- selecting only the relevant `metrics.*` files for the requested window
- decoding FTDC chunks
- reconstructing time series from delta-encoded samples
- extracting selected `opcounters.*` metrics
- returning first value, last value, and delta as JSON

---

## End-to-end flow

### 1. Set the target shards

The orchestrator reads:

`PRIMARY_TARGETS` which is a environment Variable containing the Shard list and hosts.

Expected format:

```bash
export PRIMARY_TARGETS="[('shard01','host1.example.com'),('shard02','host2.example.com')]"
```

### 2. Run the orchestrator

Example:

```bash
python3 decode_shardv4.py \
  --start 2026-07-07T10:00:00Z \
  --end 2026-07-07T11:00:00Z \
  --dir-prefix /d/d3/sem2/qa
```

### 3. Build the remote path

For each shard, the orchestrator builds:

```text
<dir-prefix>/<shard>/data/diagnostic.data
```
based on the `--dir-prefix` flag. 

Example:

```text
/d/d3/sem2/qa/shard01/data/diagnostic.data
```

### 4. Stream the helper remotely

For each `(shard, host)` pair, the orchestrator runs SSH and sends the helper script over standard input.

The helper receives:

- `--shard`
- `--dir`
- `--start`
- `--end`

### 5. Decode FTDC on the host

The helper script:

- finds `metrics.<timestamp>Z-<n>` files
- chooses the files that overlap the requested time range
- decompresses chunk payloads with `zlib`
- parses BSON reference documents
- expands delta and zero-run encoded sample data
- finds the requested metrics
- computes first/last values and delta

### 6. Aggregate results

The orchestrator saves:

- one JSON file per shard
- one combined JSON file
- one flattened CSV file

---

## Inputs

## Orchestrator inputs

### `PRIMARY_TARGETS`

Environment variable containing shard and host pairs.

Example:

```bash
[('shard01','host1.example.com'),('shard02','host2.example.com')]
```

### `--start`

Inclusive start time.

Accepted formats:

- epoch milliseconds
- `YYYY-MM-DDTHH:MM:SSZ`
- `YYYY-MM-DD HH:MM:SS`
- `YYYY-MM-DD HH:MM`
- `YYYY-MM-DD`

### `--end`

Inclusive end time, same formats as `--start`.

Must be greater than or equal to `--start`.

### `--dir-prefix`

Absolute path prefix on each remote host.

Example:

```bash
/d/d3/sem2/qa
```

---

## Helper inputs

The helper receives these arguments from the orchestrator:

### `--shard`

A shard label or number to identify the output.

### `--dir`

The full `diagnostic.data` directory for that shard.

### `--start`

Inclusive start time.

### `--end`

Inclusive end time.

---

## Target metrics

The helper reports only these metrics:

- `opcounters.command`
- `opcounters.delete`
- `opcounters.getmore`
- `opcounters.insert`
- `opcounters.query`
- `opcounters.update`

A metric is considered a match if its full name is exactly the target or ends with `.<target>`.

---

## Output files

## Per-shard output

Each shard gets:

```text
shard_outputs/<shard>.json
```

This contains:

- shard
- host
- metrics directory
- return code
- decoded data on success
- error details, stdout, and stderr on failure

## Combined JSON

```text
shard_outputs/all_shards.json
```

Contains the full aggregated result set.

## Combined CSV

```text
shard_outputs/all_shards.csv
```

Contains flattened rows with fields such as:

- `shard`
- `metric`
- `start`
- `end`
- `first_value`
- `last_value`
- `seconds`
- `delta`

---

## Helper JSON output shape

The helper prints a JSON object like this:

```json
{
  "shard": "shard01",
  "requested_start": "2026-07-07T10:00:00.000Z",
  "requested_end": "2026-07-07T11:00:00.000Z",
  "interval_seconds": 3600,
  "selected_metrics_files": [],
  "matched_samples": 0,
  "metrics": {
    "opcounters.insert": {
      "first_sample_time": "2026-07-07T10:00:00.000Z",
      "last_sample_time": "2026-07-07T11:00:00.000Z",
      "first_value": 100,
      "last_value": 4200,
      "delta": 4100
    }
  }
}
```

---

## Error handling

## Orchestrator-side failures

Common failures include:

- invalid time format
- `--end < --start`
- invalid `PRIMARY_TARGETS`
- bad SSH connectivity
- remote `python3` missing
- unreadable remote path
- helper returning non-JSON stdout
- worker exceptions during parallel execution

When this happens, the orchestrator saves the failure details in the shard JSON.

## Helper-side failures

The helper returns JSON with an `error` field when:

- `--end < --start`
- no metrics files are found
- no file matches the requested time window
- FTDC decoding fails
- BSON parsing encounters unsupported or malformed content

---

## Troubleshooting guide

### Problem: no metrics files found

Check:

- the `--dir-prefix` value
- the shard folder name
- that the remote path really ends in `data/diagnostic.data`
- that files match the expected `metrics.<timestamp>Z-<n>` naming pattern

### Problem: remote stdout was not valid JSON

Check:

- whether the helper printed logs or debug text to stdout
- the saved `stdout` and `stderr` in `shard_outputs/<shard>.json`

The helper should print only JSON to stdout.

### Problem: CSV is empty

Usually means:

- no samples matched the requested time range
- or none of the six target metrics were present

Inspect:

- `all_shards.json`
- each per-shard JSON file

### Problem: SSH command failed

Check:

- SSH access to the host
- that `python3` exists remotely on that host
- that the target directory is readable

---

## Operational checklist

Before running the toolkit, confirm:

- `PRIMARY_TARGETS` is valid
- the orchestrator script can read `decode_helper_py36.py`
- you can SSH to each host
- each host has `python3`
- the directory structure matches `<dir-prefix>/<shard>/data/diagnostic.data`
- your requested time window is correct

---

## Summary

Use the orchestrator when you want to run the same FTDC extraction across many shards in parallel.

Use the helper as the remote FTDC decoder that understands:

- metrics file selection
- BSON parsing
- zlib chunk decoding
- delta reconstruction
- opcounter extraction

The orchestrator handles fleet execution and aggregation. The helper handles FTDC decoding and metric summarization.

