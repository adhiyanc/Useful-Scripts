 # 1. Bsondump method ( decode_ftdc.sh) and Ruby

```
./[Script_Name] [input metric file] [output JSON file]
```
Example : to generate only `JSON` output

```
./decode_ftdc.sh "/Users/adhiyan.chattopadhyay/Desktop/locates/data/replset/rs1/db/diagnostic.data/metrics.2026-06-22T12-11-36Z-00000" decoded_ftdc.json
```

Example : to generate `JSON` output and use the file to produce report with predefined set of Metrics

```
./decode_ftdc.sh "<input_metric_file>" <filename>.json && python3 report.py
```

# 2.  FTDC Metrics Reporter ( Python script ftdc_metrics_report_v5.py )

## Overview

`ftdc_metrics_report.py` is a single-file Python CLI that reads MongoDB FTDC data and prints a concise Markdown-style performance report.

It supports both raw FTDC input and pre-decoded JSON snapshots.

The script is designed for quick triage and review of a performance window without relying on `bsondump` or third-party Python packages.

## What the script does

The script has two execution paths:

* Raw FTDC path
  * reads a single `metrics.*` file or a full `diagnostic.data` directory
  * decodes outer BSON documents with an internal minimal BSON decoder
  * selects FTDC type 1 metric chunks
  * zlib-decompresses chunk payloads
  * reconstructs FTDC sample deltas from the reference document and varint delta stream
  * flattens only the metric paths needed by the report
  * summarizes the resulting sample rows into a timeframe report

* Decoded JSON path
  * reads concatenated JSON snapshots from an already-decoded file
  * normalizes the required fields into the same row structure used by the raw FTDC path
  * runs the same summary logic as the raw path

## Supported input modes

### 1. Single raw FTDC metrics file

Example:

```bash
python3 ftdc_metrics_report.py /path/to/metrics.2026-05-11T04-19-11Z-00000
```

### 2. `diagnostic.data` directory

Example:

```bash
python3 ftdc_metrics_report.py /path/to/diagnostic.data
```

Behavior:

* processes `metrics.*` files in sorted order
* skips `metrics.interim`
* treats the directory as one continuous FTDC archive stream

### 3. Decoded JSON snapshots

Example:

```bash
python3 ftdc_metrics_report.py /path/to/xaa.json
```

or force decoded mode explicitly:

```bash
python3 ftdc_metrics_report.py /path/to/snapshots.txt --decoded-json
```

Notes:

* `.json` input is auto-treated as decoded JSON
* `--decoded-json` is useful when the decoded file does not use a `.json` suffix

### Summary

Includes:

- Opcounters
- Op latencies
- WT Cache stats
- CPU and Memory stats. 

## Typical examples

### Analyze one raw metrics file

```bash
python3 ftdc_metrics_report.py /path/to/metrics.2026-05-11T04-19-11Z-00000
```

### Analyze a full `diagnostic.data` directory

```bash
python3 ftdc_metrics_report.py /path/to/diagnostic.data
```

### Analyze decoded JSON

```bash
python3 ftdc_metrics_report.py /path/to/decoded_ftdc.json
```

### Force decoded JSON mode

```bash
python3 ftdc_metrics_report.py /path/to/decoded_snapshots.txt --decoded-json
```

## Best use case

This script is best used when you want a fast, human-readable review of a MongoDB FTDC capture window, especially for support or troubleshooting workflows where raw FTDC exists but a full FTDC toolchain is inconvenient.
