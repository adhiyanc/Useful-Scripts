1. Usage of Bsondump method 

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
