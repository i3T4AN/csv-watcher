# CSV to JSON Watcher

A Python utility that monitors directories for CSV files and automatically converts them into JSON or JSON Lines format.  
It supports recursive watching, delimiter detection, overwrite or unique naming, and works in real-time with watchdog or a fallback polling mode.

## Features
- Watches a directory (and optionally subdirectories) for new or modified CSV files.
- Converts CSV files to:
  - Standard JSON arrays (.json)
  - JSON Lines format (.jsonl)
- Auto-detects CSV delimiters and quote characters, with manual overrides.
- Ensures files are stable before conversion to avoid partial reads.
- Ignores temporary/incomplete files (e.g., .tmp, .part, .crdownload).
- Supports overwrite or auto-unique output naming.
- Works with or without watchdog installed.

## Requirements
- Python 3.8+
- watchdog (optional, for efficient filesystem event monitoring)

Install the optional dependency:
pip install watchdog

## Usage
Run the script from the command line:

python csv_watcher.py --watch <directory> [options]

### Arguments
- --watch (required): Directory to monitor for CSV files.
- --out: Directory for JSON output (default: same as --watch).
- --recursive: Monitor subdirectories.
- --process-existing: Convert existing CSVs at startup.
- --jsonl: Write JSON Lines format (.jsonl) instead of array JSON.
- --overwrite: Overwrite existing files instead of uniquifying names.
- --indent: Indent level for pretty JSON output (array mode only).
- --delimiter: Override CSV delimiter (default: auto-detect).
- --quotechar: Override CSV quote character (default: auto-detect).
- --encoding: Input file encoding (default: utf-8-sig).
- --log: Log level (DEBUG, INFO, WARNING, ERROR).

### Example
Watch ./input for CSV files and convert them into JSON Lines in ./output:
python csv_watcher.py --watch ./input --out ./output --jsonl --process-existing

### Example with pretty JSON
python csv_watcher.py --watch ./data --indent 2

## Notes
- If watchdog is not installed, the script falls back to polling mode, checking files every 2 seconds.
- Temporary or incomplete files are ignored until they stabilize.
