<p align="center">
  <img src="media/logo.png" alt="Tango Logo" width="400">
</p>

<h1 align="center">Tango</h1>

<p align="center">
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-stable-orange.svg" alt="Rust"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

<p align="center"><strong>High-performance multi-format log parser for security, sysops, and devops.</strong></p>

Tango automatically detects and parses JSON, Logfmt, Syslog, Apache, Android logcat, and more — normalizing everything into a unified format for analysis.

## Features

- 🔍 **Auto-detection** — No format configuration needed
- ⚡ **Fast** — Written in Rust, handles large files efficiently  
- 🔄 **Unified output** — Normalize heterogeneous logs to JSON/CSV
- 🎯 **Rich filtering** — By level, time, pattern, fields
- 📊 **Built-in analytics** — Histograms, top-N, aggregations
- 🔗 **Pipe-friendly** — Works with grep, jq, and friends

## Install

```bash
make build
sudo make install
```

## Quick Start

```bash
# Parse logs
tango parse /var/log/auth.log

# Search for patterns
tango search /var/log/*.log --grep "error" --level error

# Get statistics
tango stats /var/log/syslog --top 10 --by hostname

# Live tail with parsing
tango tail -f /var/log/messages --level error

# Convert to JSON for SIEM
tango convert /var/log/*.log -f ndjson -o events.json
```

## Commands

| Command | Purpose |
|---------|---------|
| `parse` | Parse and display logs with filters |
| `search` | Search logs with grep, context lines |
| `stats` | Statistics, histograms, aggregations |
| `tail` | Live tail with real-time parsing |
| `convert` | Convert/merge logs to JSON/CSV/NDJSON |

## Use Cases

### Security Analyst

```bash
# Find failed SSH logins
tango search /var/log/auth.log --grep "Failed password" -H

# Hunt for break-in attempts
tango search /var/log/secure --grep "BREAK-IN" --level error

# Export to SIEM
tango parse /var/log/*.log --level error,warn -o json > alerts.json
```

### SysOps / SRE

```bash
# Tail multiple logs, errors only
tango tail -f /var/log/syslog --level error

# Time-based histogram
tango stats /var/log/messages --histogram --bucket hour

# Filter by time range
tango parse app.log --since "1 hour ago" --until "now"
```

### DevOps

```bash
# Merge heterogeneous logs by timestamp
tango convert app.log nginx.log syslog --merge -o combined.json

# Top errors by component
tango stats /var/log/app.log --top 10 --by tag

# Filter by field
tango parse logs/*.log -F hostname=prod-01 -F level=error
```

## Key Options

**Filtering:**
- `--level error,warn` — Filter by severity
- `--grep "pattern"` — Search in message
- `-F field=value` — Filter by extracted field
- `--since "1 hour ago"` — Time range start
- `--until "2025-01-01"` — Time range end

**Output:**
- `-o table|json|ndjson|csv` — Output format
- `--highlight` — Highlight matches
- `-n 100` — Limit results
- `--no-raw` — Exclude raw line

**Analysis:**
- `--count-by field` — Count by field value
- `--top N --by field` — Top N values
- `--histogram --bucket hour` — Time distribution
- `--unique field` — List unique values

## Supported Formats

Auto-detected:
- **JSON** — Structured JSON logs
- **Logfmt** — `key=value` format
- **Syslog** — Linux system logs
- **Apache** — Error logs
- **Android** — Logcat format
- **OpenSSH** — Auth logs

## Output

All logs normalized to:

```json
{
  "timestamp": "2025-01-01T12:00:00Z",
  "level": "error",
  "message": "Connection refused",
  "fields": {"hostname": "prod-01", "pid": 1234},
  "format": "Pattern"
}
```

## Build

```bash
make build       # Build release
make test        # Run tests
make install     # Install to /usr/local/bin
make uninstall   # Remove
make help        # All targets
```

## Similar Tools

- **[lnav](https://lnav.org)** — Full-featured TUI log viewer
- **[angle-grinder](https://github.com/rcoh/angle-grinder)** — Aggregation pipelines
- **[jq](https://stedolan.github.io/jq/)** — JSON processing

Tango fills the gap between raw `grep/awk` and heavy log platforms like Splunk/ELK.

## Contributing

PRs welcome! Please run `make test` and `make lint` before submitting.

## License

MIT
