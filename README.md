# Observation Planning Tools

This repository contains Python scripts for planning and executing astronomical observations with GroundBIRD.

| Script | Role |
|---|---|
| `planner.py` | Generates a 24-hour observation timing schedule in UTC |
| `reset_scheduler.py` | Executes reset commands for a **single target** over one 24-hour period (interactive) |
| `continuous_scheduler.py` | Continuously schedules **multiple targets** with priority control (long-running, no interaction required) |
| `mailer.py` | Sends scheduled observation report emails (used internally by `continuous_scheduler.py`) |

---

## Quick Start — continuous_scheduler.py

This is the recommended way to run long-term automated observations.

### 1. Edit the config file

```yaml
# priority.yaml
targets:
  - name: moon
    priority: 2
    elevation: 70
  - name: jupiter
    priority: 1
    elevation: 70

report:
  weekdays: [0, 1, 2, 3, 4, 5, 6]   # 0=Mon … 6=Sun
  time_utc: "08:00"
  report_days_ahead: 7
  to_addr:
    - your@email.address
```

Priority numbers: **1 = highest**.  While a higher-priority target is above its elevation line, lower-priority targets are skipped.

### 2. Start in a screen session

```bash
# Create (or reattach to) a screen session
screen -R auto

# Run the scheduler
./continuous_scheduler.py

# Detach (keep running in background): Ctrl+A, then D

# Reattach later
screen -r auto
```

Custom config file:
```bash
./continuous_scheduler.py --config my_config.yaml
```

Check which targets are currently being observed:
```bash
./continuous_scheduler.py --list
```

Stop the scheduler: reattach to the screen session and press `Ctrl+C`, or send SIGTERM/SIGHUP.

### 3. Log file

Logs are written to `./tmp/continuous_scheduler.log`.

---

## planner.py

Generates a 24-hour observation timing schedule for a given celestial body.

```bash
python planner.py <body> [--date DATE] [--line ELEVATION] [--sun SUN_ANGLE]

Arguments:
  body        Astronomical body name (e.g. 'moon', 'jupiter')
  --date      Observation date (default: current time)
  --line, -l  Elevation angle of the telescope (default: 70 degrees)
  --sun,  -s  Sun avoidance elevation angle (default: 60 degrees)
```

Example:
```bash
./planner.py moon
./planner.py jupiter --date "2025-03-01" --line 65 --sun 55
```

Example output:
```
Time (UTC)               Action
2025-03-01 02:15        Restart GB01 for jupiter-rise
2025-03-01 02:30        Restart GB02 for jupiter-rise
...
2025-03-01 05:30        Close dome to avoid sun
```

---

## reset_scheduler.py

Executes reset commands for a **single target** over one 24-hour period.  
Displays the schedule and asks for confirmation before running.  
Use `continuous_scheduler.py` instead for long-term multi-target operation.

```bash
./reset_scheduler.py [--target TARGET] [--elevation ELEVATION] [--sun-avoid SUN_ANGLE]

Arguments:
  --target        Target celestial body (default: moon)
  --elevation     Elevation angle for observations (default: 70 degrees)
  --sun-avoid     Sun avoidance angle (default: 60 degrees)
  --list          List currently active observation targets and exit
```

Running with screen:
```bash
screen -R auto
./reset_scheduler.py --target moon
# Detach: Ctrl+A, D
screen -r auto
```

Log file: `reset_scheduler.log`

---

## How It Works — Internals

### planner.py

Calculates the times when a celestial body crosses a given elevation angle (rise and set), using `astropy` with the Observatorio del Teide coordinates. For each crossing, 7 GB restart times are generated with staggered offsets (defined in `DF_RISE_MIN` / `DF_SET_MIN`). Sun crossing times at the avoidance angle are also included. Returns a `pandas.DataFrame` sorted by time.

### reset_scheduler.py

Single-target, single-day, **interactive** scheduler. Calls `planner.py`, displays the schedule, asks "Y/N", then executes each reset command at the scheduled time via `daq_client.py`. After each reset, a sky command is sent automatically after 20 minutes. A lock file under `lock/` prevents duplicate schedulers for the same target.

### continuous_scheduler.py

**Non-interactive, continuous** multi-target scheduler. Key behaviours:

- **Daily replanning**: At the end of each 24-hour cycle, the schedule is recalculated automatically.
- **Priority control**: When a GB issues a reset for a target, that GB is marked as "active" for 60 minutes. During this window, reset commands from lower-priority targets for the same GB are skipped.
- **Sky command**: 20 minutes after each reset, the target name is set back to `sky:<shifter>` (shifter name is preserved from the current DAQ status).
- **Stagger**: A 30-second delay is inserted between successive GB resets to avoid simultaneous CPU load.
- **DAQ guard**: Reset commands are only sent when the DAQ is already running (`nojobs` check). If the DAQ is stopped, the event is skipped without error.
- **Lock files**: One lock file per target is created at startup and removed on exit (SIGINT / SIGTERM / SIGHUP handled).
- **Email reports** (`mailer.py`): If a `report:` section is present in `priority.yaml`, a background asyncio task sends a weekly (or daily) schedule email via Gmail. The email includes the server hostname (`os.uname().nodename`), the priority order, reset command counts per target, and a full chronological schedule for the coming period.

### Lock files

Lock files are stored in `lock/` and contain `PID,username,timestamp`. Stale locks (process no longer exists) are removed automatically at startup. Use `--list` to inspect active locks.

---

## Notes

- All scripts assume the observatory location is **Observatorio del Teide**.
- All times are in **UTC**.
- The target name is set to `sky` every 20 minutes after each reset command.
- Python 3.10+ is required (`str | None` type union syntax).
