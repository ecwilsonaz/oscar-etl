# oscar-etl

Extract clean CSVs from your OSCAR CPAP data.

---

## Why this exists

The manual OSCAR export gives you three flat CSV dumps -- a daily summary, per-session stats, and a massive interleaved details file mixing pressure readings with apnea events in a narrow format. It only includes three signals (pressure, EPAP, flow limitation), omits leak, respiratory rate, tidal volume, minute ventilation, and snore entirely, and uses BiPAP column headers that are always zero for your APAP machine. Session boundaries are also only available through the OSCAR GUI -- the CSVs don't make it easy to correlate events with specific mask-on segments.

oscar-etl reads the same raw EDF files OSCAR does, but produces cleaner, more complete output. It splits sessions by each mask-on segment (matching OSCAR's internal model), gives you seven signals in a wide-format timeseries CSV (one row per 2-second timestamp instead of one row per measurement), and separates events into their own file for easy correlation with sleep-tracker data. Daily stats are recomputed from the raw signal data across all sessions -- not approximated from the longest session or averaged. It also captures events that happen during mask-off gaps (which OSCAR's export silently drops), and the whole thing runs in about a minute with no manual clicking.

## Quick Start

```bash
pip install oscar-etl
```

```bash
oscar-etl
```

Expected output:

```
  oscar-etl v0.1.0

  Data source
  -----------------------------------
  Location:  /Users/you/OSCAR_Data
  Profile:   Default
  Machine:   39123456789
  EDF files: 2,847

  Extracting
  -----------------------------------
  Discovering sessions           ━━━━━━━━━━━━━━━━ 100%
  Parsing EDF files              ━━━━━━━━━━━━━━━━ 100%
  Writing sessions.csv           ━━━━━━━━━━━━━━━━ 100%
  Writing daily.csv              ━━━━━━━━━━━━━━━━ 100%
  Writing events.csv             ━━━━━━━━━━━━━━━━ 100%
  Writing timeseries.csv         ━━━━━━━━━━━━━━━━ 100%

  Output
  -----------------------------------
  oscar-etl-output/
    cpap_sessions.csv             42 KB
    cpap_daily.csv                18 KB
    cpap_events.csv               1.2 MB
    cpap_timeseries.csv           812.4 MB

  Done in 58s
```

Four CSV files land in `./oscar-etl-output/`. See [Output Files](#output-files) for what each contains.

## macOS Setup

On macOS, OSCAR stores data in `~/Documents/OSCAR_Data/`, which is protected by the OS. You need to grant Terminal Full Disk Access temporarily to move the data somewhere unprotected. There are two options.

### Option A: Symlink (recommended)

This moves your data out of the protected `Documents` folder and symlinks it back so OSCAR still finds it. You only need to do this once, and afterward no special permissions are required.

1. Open **System Settings > Privacy & Security > Full Disk Access** and enable **Terminal**.

2. Move the data and create a symlink:

   ```bash
   mv ~/Documents/OSCAR_Data ~/OSCAR_Data
   ln -s ~/OSCAR_Data ~/Documents/OSCAR_Data
   ```

3. Open **System Settings > Privacy & Security > Full Disk Access** and disable **Terminal**.

4. Verify it works:

   ```bash
   ls ~/OSCAR_Data
   ```

oscar-etl now reads from `~/OSCAR_Data` permanently, no special permissions needed. OSCAR itself continues to work because the symlink points it to the new location.

### Option B: Leave Full Disk Access on

If you prefer not to move files around, just leave Full Disk Access enabled for Terminal. oscar-etl will read directly from `~/Documents/OSCAR_Data/`.

### Windows and Linux

No special setup is needed. oscar-etl auto-discovers the OSCAR data directory in the standard locations.

## Output Files

All output goes to `./oscar-etl-output/` by default (override with `--output-dir`).

### cpap_sessions.csv

One row per mask-on segment.

| Column | Description |
|---|---|
| `date` | Night date (see [evening date convention](#evening-date-convention)) |
| `session_start` | ISO 8601 timestamp when mask went on |
| `session_end` | ISO 8601 timestamp when mask came off |
| `duration_minutes` | Session length in minutes |
| `ahi` | Apnea-hypopnea index for this session |
| `ca_count` | Central apnea count |
| `oa_count` | Obstructive apnea count |
| `h_count` | Hypopnea count |
| `ua_count` | Unclassified apnea count |
| `arousal_count` | Arousal/RERA count |
| `pressure_median` | Median pressure (cmH2O) |
| `pressure_95` | 95th percentile pressure |
| `pressure_995` | 99.5th percentile pressure |
| `leak_median` | Median leak rate (L/min) |
| `leak_95` | 95th percentile leak rate |
| `resp_rate_median` | Median respiratory rate (breaths/min) |
| `tidal_vol_median` | Median tidal volume (mL) |
| `minute_vent_median` | Median minute ventilation (L/min) |

### cpap_daily.csv

One row per night, aggregated from all sessions for that date.

| Column | Description |
|---|---|
| `date` | Night date |
| `sessions` | Number of mask-on segments |
| `start` | Earliest session start |
| `end` | Latest session end |
| `total_minutes` | Total therapy time across all sessions |
| `ahi` | AHI across all sessions |
| Same stat columns as sessions | Aggregated across all sessions for the night |

### cpap_events.csv

One row per apnea, hypopnea, or arousal event.

| Column | Description |
|---|---|
| `datetime` | ISO 8601 timestamp of the event |
| `date` | Night date the event belongs to |
| `session_start` | The session this event occurred in |
| `event` | Event type (e.g., `OA`, `CA`, `H`, `UA`, `arousal`) |
| `duration_sec` | Duration of the event in seconds |

### cpap_timeseries.csv

One row per 2-second sample, wide format. This file is large (typically around 800 MB for a year of data). Use `--skip-timeseries` to skip it.

| Column | Description |
|---|---|
| `datetime` | ISO 8601 timestamp |
| `date` | Night date |
| `session_start` | The session this sample belongs to |
| `pressure` | Therapy pressure (cmH2O) |
| `leak` | Leak rate (L/min) |
| `resp_rate` | Respiratory rate (breaths/min) |
| `tidal_vol` | Tidal volume (mL) |
| `minute_vent` | Minute ventilation (L/min) |
| `snore` | Snore index |
| `flow_limit` | Flow limitation index |

### Evening date convention

Sessions starting before noon are attributed to the previous calendar day. For example, a session that starts at 1:00 AM on March 16 belongs to the "night of March 15." This matches OSCAR and most sleep tracker conventions.

Shift workers or anyone with a non-standard sleep schedule can adjust the cutoff with the `--day-boundary` flag. For example, `--day-boundary 6` attributes sessions starting before 6 AM to the previous day.

## CLI Reference

```
oscar-etl [OPTIONS]
```

| Flag | Description |
|---|---|
| `--oscar-dir PATH` | Path to OSCAR_Data directory. Auto-discovered from standard locations if omitted. |
| `--output-dir PATH` | Output directory. Default: `./oscar-etl-output/` |
| `--profile NAME` | Profile name. If omitted and multiple profiles exist, an interactive picker is shown. |
| `--machine SERIAL` | Machine serial number filter. |
| `--skip-timeseries` | Skip the large timeseries CSV (~800 MB). |
| `--day-boundary HOUR` | Hour (0--23) that separates sleep nights. Default: `12`. |
| `--version` | Show version and exit. |

## Supported Machines

**ResMed only**, tested with AirSense 10 AutoSet.

Other ResMed models that use the same EDF-based data format should work. The tool warns on unrecognized signal labels rather than crashing, so you can try it and see what happens.

PRs welcome from users with other machines (Philips, Fisher & Paykel, etc.) who can build and test against their own data.

## Contributing

Contributions are welcome. Here is a quick orientation for the codebase:

- **Signal label mapping** lives in `src/oscar_etl/etl.py` (`PLD_SIGNAL_MAP`). This is the dictionary that maps ResMed EDF signal labels to the normalized column names used in the output CSVs.
- **EDF parser** lives in `src/oscar_etl/edf.py`. It is format-agnostic and reads any valid EDF file.
- **To add support for a new machine**, you would add signal label mappings for that machine's EDF signal names and test against real data from that machine.

Issues and PRs are welcome on GitHub. If you are adding support for a new machine, please include sample output (anonymized) so reviewers can verify correctness.

## License

MIT -- see [LICENSE](LICENSE) for details.
