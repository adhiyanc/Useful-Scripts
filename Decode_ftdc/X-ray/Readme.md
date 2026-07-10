### FTDC Analysis Component

The FTDC baseline analysis reports its capture timespan and effective sample rate, then
groups metrics into Workload, Read/Write Operations and Latencies, and
Performance sections. It includes operation rates and latencies, host memory
and CPU utilization, WiredTiger cache utilization, queue depth for each block
device, and free-space and utilization charts for every reported mount point.
Each metric shows its peak, average, unit, and a chart saved under the report
output's `charts` directory.
Start and end are inclusive UTC ISO-8601 timestamps. When omitted, the first
and last data points in the archive are used.

```bash
x-ray ftdc /var/lib/mongo/diagnostic.data
x-ray ftdc /var/lib/mongo/diagnostic.data 2026-06-17T08:00:00Z 2026-06-17T10:00:00Z
```

```bash
x-ray ftdc [-h] [-s CHECKSET] [-o OUTPUT] [-f {markdown,html}] [-r RATE] ftdc_path [start_time] [end_time]
```
