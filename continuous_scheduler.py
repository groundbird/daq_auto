#!/usr/bin/env python3
"""
continuous_scheduler.py — GroundBIRD Continuous Multi-Target Observation Scheduler
"""

import asyncio
import errno
import logging
import os
import re
import signal
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from os.path import dirname, abspath, join

import pandas as pd
import yaml
from astropy.time import Time

from planner import get_plan_oneday
from mailer import email_report_loop

# ── Logging ───────────────────────────────────────────────────────────────────
_SCRIPT_DIR = dirname(abspath(__file__))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(join(_SCRIPT_DIR, "tmp", "continuous_scheduler.log")),
        logging.StreamHandler(),
    ],
)

# ── Constants ─────────────────────────────────────────────────────────────────
LOCK_DIR              = "/home/gb/obstool/daq_auto/lock"
SCHEDULER_LOCK_FILE   = join(_SCRIPT_DIR, ".continuous_scheduler.lock")
DAQ_CLIENT_PATH       = "/home/gb/obstool/daq_client.py"
SKY_DELAY_MINUTES     = 20
STAGGER_SECONDS       = 30
OBS_DURATION_MINUTES  = 60   # how long one observation run lasts

_ACTIVE_LOCK_FILES: list[str] = []
_sky_tasks:    dict[str, asyncio.Task] = {}  # gb_number → pending sky task
_active_tasks: dict[str, asyncio.Task] = {}  # gb_number → pending active-target expiry task
_active_target: dict[str, str]         = {}  # gb_number → currently observing target name

sys.path.insert(0, os.path.dirname(DAQ_CLIENT_PATH))
from daq_client import CtrlDaqClient  # noqa: E402


# ═════════════════════════════════════════════════════════════════════════════
# Signal handler
# ═════════════════════════════════════════════════════════════════════════════

def _cleanup_and_exit(sig, frame):
    for lf in _ACTIVE_LOCK_FILES:
        if os.path.exists(lf):
            os.remove(lf)
    _remove_scheduler_lock()
    logging.info("Scheduler terminated.")
    sys.exit(0)


# ═════════════════════════════════════════════════════════════════════════════
# DAQ socket helpers
# ═════════════════════════════════════════════════════════════════════════════

def _daq_ctrl(gb_number: str, command_suffix: str, comment: str | None = None) -> str:
    """
    Send one low-level command to the DAQ socket for GB{gb_number} and return
    the decoded response.  A fresh TCP connection is opened for every call
    (matches daq_client.py behaviour).

    Examples
    --------
    _daq_ctrl("01", "statu")            →  sends  "k#daqGB01statu"
    _daq_ctrl("01", "targt", "sky:bob") →  sends  "k#daqGB01targt sky:bob"
    _daq_ctrl("01", "reset")            →  sends  "k#daqGB01reset"
    """
    try:
        cl  = CtrlDaqClient()
        word = f"GB{gb_number}{command_suffix}"
        raw  = cl.ctrl(word if comment is None else f"{word} {comment}")
        return raw.decode(errors="replace").strip()
    except Exception as exc:
        logging.error(f"DAQ socket error GB{gb_number} '{command_suffix}': {exc}")
        return ""


def _is_daq_running(gb_number: str) -> bool:
    """
    Return True when the DAQ for GB{gb_number} is currently active.

    Responses from the server:
        Running: mainjob:['7885'],subjob:['11795', '11804'],targetname:"sky:miku"
        Stopped: nojobs
    """
    resp = _daq_ctrl(gb_number, "statu")
    logging.debug(f"[STATUS] GB{gb_number}: {resp!r}")
    return "nojobs" not in resp


def _get_shifter(gb_number: str) -> str:
    """
    Query the DAQ for the *current* shifter name and return it.

    The status reply contains:  targetname:"<target>:<shifter>"
    e.g.  targetname:"test:shonda"  →  returns "shonda"

    Returns an empty string if the response cannot be parsed.
    """
    resp = _daq_ctrl(gb_number, "statu")
    m = re.search(r'targetname:"([^":]+):([^"]+)"', resp)
    if m:
        return m.group(2).strip()
    # No colon in targetname (e.g. targetname:"sky") — match daq_client.py behaviour
    # which appends "none" when the shifter field is absent.
    logging.warning(f"[SHIFTER] No shifter found for GB{gb_number} ({resp!r}), using 'none'")
    return "none"


# ═════════════════════════════════════════════════════════════════════════════
# Lock management
# ═════════════════════════════════════════════════════════════════════════════

def _lock_path(target_name: str) -> str:
    return os.path.join(LOCK_DIR, f"{target_name}.lock")


def check_lock(target_name: str) -> bool:
    """Return True if a valid lock already exists for target_name."""
    path = _lock_path(target_name)
    if not os.path.exists(path):
        return False

    with open(path) as f:
        parts = f.read().strip().split(",")
    pid, username, timestamp = int(parts[0]), parts[1], parts[2]

    try:
        os.kill(pid, 0)  # signal 0: check process existence without sending a signal
    except OSError as exc:
        if exc.errno == errno.ESRCH:  # ESRCH = No such process → stale lock
            os.remove(path)
            return False
        raise  # any other OSError (e.g. permission denied) is unexpected → raise

    print(f"ERROR: '{target_name}' already monitored by {username} (PID {pid}) since {timestamp}")
    return True


def create_lock(target_name: str) -> str:
    """Create a lock file and return its path. Exits if lock already exists."""
    if check_lock(target_name):
        print(f"Cannot acquire lock for '{target_name}'. Exiting.")
        sys.exit(1)

    os.makedirs(LOCK_DIR, exist_ok=True)
    path = _lock_path(target_name)
    pid  = os.getpid()
    user = os.environ.get("USER", f"pid-{pid}")

    with open(path, "w") as f:
        f.write(f"{pid},{user},{datetime.now().isoformat()}")
    os.chmod(path, 0o666)
    _ACTIVE_LOCK_FILES.append(path)
    logging.info(f"Lock acquired: {target_name}")
    return path


def remove_lock(path: str):
    if path and os.path.exists(path):
        os.remove(path)
        if path in _ACTIVE_LOCK_FILES:
            _ACTIVE_LOCK_FILES.remove(path)
        logging.info(f"Lock removed: {path}")


def list_active_targets():
    if not os.path.exists(LOCK_DIR):
        print(f"Lock directory {LOCK_DIR} does not exist.")
        return

    rows = []
    for fn in os.listdir(LOCK_DIR):
        if not fn.endswith(".lock") or fn.startswith("."):
            continue
        tname = fn[:-5]
        fpath = os.path.join(LOCK_DIR, fn)
        with open(fpath) as f:
            parts = f.read().strip().split(",")
        pid, user, ts = int(parts[0]), parts[1], parts[2]
        try:
            os.kill(pid, 0)
            status = "Running"
        except OSError:
            status = "Unknown"
        rows.append((tname, user, pid, ts, status))

    if rows:
        print("\n=== Active Observation Locks ===")
        for tname, user, pid, ts, status in rows:
            print(f"  {tname:12s}  owner={user}  PID={pid}  since={ts}  [{status}]")
    else:
        print("No active observation locks found.")


def _write_scheduler_lock():
    with open(SCHEDULER_LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def _remove_scheduler_lock():
    if os.path.exists(SCHEDULER_LOCK_FILE):
        os.remove(SCHEDULER_LOCK_FILE)

# ═════════════════════════════════════════════════════════════════════════════
# Priority check
# ═════════════════════════════════════════════════════════════════════════════

def is_blocked(gb_number: str, target_name: str, target_configs: list[dict]) -> bool:
    """
    Return True if THIS specific GB is currently running a higher-priority target.
    """
    active = _active_target.get(gb_number)
    if active is None:
        return False   # this GB is idle → not blocked

    my_priority   = next(t["priority"] for t in target_configs if t["name"] == target_name)
    priority_map  = {t["name"]: t["priority"] for t in target_configs}
    active_priority = priority_map.get(active, 999)

    if active_priority < my_priority:
        logging.info(
            f"[SKIP] GB{gb_number} {target_name}: "
            f"currently running {active} (priority {active_priority})"
        )
        return True
    return False


# ═════════════════════════════════════════════════════════════════════════════
# Command execution
# ═════════════════════════════════════════════════════════════════════════════

async def _expire_active_target(gb_number: str):
    """
    Remove gb_number from _active_target after OBS_DURATION_MINUTES.
    This marks the GB as free so that lower-priority targets are no longer
    blocked by this GB after one full observation run (60 min) has elapsed.

    CancelledError is caught because a new reset on the same GB will cancel
    this timer and start a fresh one.
    """
    try:
        await asyncio.sleep(OBS_DURATION_MINUTES * 60)
    except asyncio.CancelledError:
        logging.info(f"[ACTIVE] GB{gb_number} expiry timer cancelled (new reset incoming)")
        return

    target = _active_target.pop(gb_number, None)
    logging.info(f"[ACTIVE] GB{gb_number} observation expired ({target} → free)")


async def _delayed_sky(gb_number: str):
    """
    Send --targetname sky (no --reset) after SKY_DELAY_MINUTES,
    *only* when the DAQ is still running at that point.

    The current shifter is read just before the command is sent so that
    this scheduler never overwrites the shifter name.

    CancelledError is caught here because cancellation is an expected, normal
    event (a new reset arrived for the same GB before the timer fired).
    """
    try:
        await asyncio.sleep(SKY_DELAY_MINUTES * 60)
    except asyncio.CancelledError:
        logging.info(f"[SKY] GB{gb_number} sky timer cancelled (new reset incoming)")
        return

    # ── Guard: skip if DAQ is not running ────────────────────────────────────
    resp = _daq_ctrl(gb_number, "statu")
    if "nojobs" in resp:
        logging.info(f"[SKY] GB{gb_number} DAQ not running — sky command skipped")
        return
    # ── Preserve the current shifter ─────────────────────────────────────────
    m           = re.search(r'targetname:"([^":]+):([^"]+)"', resp)
    shifter     = m.group(2).strip() if m else "none"
    targt_value = f"sky:{shifter}" if shifter else "sky"

    resp = _daq_ctrl(gb_number, "targt", targt_value)
    if resp:
        logging.info(f"[SKY] GB{gb_number} → targetname set to {targt_value!r}")
    else:
        logging.error(f"[SKY] GB{gb_number} → targetname command failed")


def execute_reset(gb_number: str, target_name: str):
    """
    Set the target name (preserving the current shifter) and reset the DAQ,
    but *only* when the DAQ is already running.
    """
    resp = _daq_ctrl(gb_number, "statu")

    if "nojobs" in resp:
        logging.info(f"[SKIP] GB{gb_number} DAQ not running — reset for {target_name} skipped")
        return

    logging.debug(f"[STATUS] GB{gb_number}: {resp!r}")

    m = re.search(r'targetname:"([^":]+):([^"]+)"', resp)
    shifter     = m.group(2).strip() if m else "none"
    targt_value = f"{target_name}:{shifter}" if shifter else target_name

    logging.info(f"[RESET] GB{gb_number} → setting targetname to {targt_value!r}, then reset")

    _daq_ctrl(gb_number, "targt", targt_value)

    result = _daq_ctrl(gb_number, "reset")


    if "failed" in result.lower():
        logging.error(f"[RESET] GB{gb_number} reset failed: {result!r}")
        return

    logging.info(f"[RESET] GB{gb_number} reset issued (target={target_name}, shifter={shifter!r})")

    # Record which target this GB is now observing.
    # is_blocked() reads this to decide whether to skip lower-priority targets.
    _active_target[gb_number] = target_name

    # Cancel any previous expiry timer and start a fresh 60-minute one.
    if gb_number in _active_tasks and not _active_tasks[gb_number].done():
        _active_tasks[gb_number].cancel()
    _active_tasks[gb_number] = asyncio.create_task(_expire_active_target(gb_number))

    # Cancel any pending sky task for this GB before scheduling a new one.
    if gb_number in _sky_tasks and not _sky_tasks[gb_number].done():
        _sky_tasks[gb_number].cancel()
    _sky_tasks[gb_number] = asyncio.create_task(_delayed_sky(gb_number))


# ═════════════════════════════════════════════════════════════════════════════
# Daily schedule
# ═════════════════════════════════════════════════════════════════════════════

def _parse_gb_number(action: str) -> str | None:
    """Extract GB number string from action text, e.g. 'Restart GB01 for ...' → '01'."""
    if "Restart GB" not in action:
        return None
    return action.split("GB")[1][:2]


def _build_events(target_configs: list[dict]) -> list[dict]:
    """Build a time-sorted list of events for all targets over the next 24 hours."""
    now    = Time.now()
    events = []

    for tcfg in target_configs:
        tname = tcfg["name"]
        table = get_plan_oneday(
            time=now,
            line=tcfg["elevation"],
            body_name=tname,
        )
        for _, row in table.iterrows():
            evt_time = pd.to_datetime(row["Time (UTC)"])
            if evt_time.tzinfo is None:
                evt_time = evt_time.replace(tzinfo=timezone.utc)
            action: str = row["Action"]
            events.append(dict(
                time=evt_time,
                target=tname,
                priority=tcfg["priority"],
                gb_number=_parse_gb_number(action),
                action=action,
            ))

    events.sort(key=lambda e: e["time"])
    return events


async def run_one_day(target_configs: list[dict]):
    events  = _build_events(target_configs)
    now_utc = datetime.now(timezone.utc)
    day_end = now_utc + timedelta(hours=24)

    print("\n" + "=" * 60)
    print("  Today's Schedule")
    print("=" * 60)
    for ev in events:
        if ev["gb_number"] is None:
            continue
        print(f"  {ev['time'].strftime('%Y-%m-%d %H:%M UTC')}  [{ev['target']:10s}]  {ev['action']}")
    print("=" * 60 + "\n")

    for ev in events:
        if ev["gb_number"] is None:
            continue
        evt_time: datetime = ev["time"]

        if evt_time < now_utc:
            continue
        if evt_time > day_end:
            break

        sleep_secs = (evt_time - datetime.now(timezone.utc)).total_seconds()
        if sleep_secs > 1:
            logging.info(f"Waiting {sleep_secs:.0f}s → [{ev['target']}] {ev['action']}")
            await asyncio.sleep(sleep_secs)

        if is_blocked(ev["gb_number"], ev["target"], target_configs):
            continue

        execute_reset(ev["gb_number"], ev["target"])
        await asyncio.sleep(STAGGER_SECONDS)


# ═════════════════════════════════════════════════════════════════════════════
# Main loop
# ═════════════════════════════════════════════════════════════════════════════

async def run_continuous(target_configs: list[dict], report_cfg: dict | None):
    lock_files = [create_lock(tcfg["name"]) for tcfg in target_configs]

    if report_cfg is not None:
        asyncio.create_task(email_report_loop(report_cfg, target_configs))

    try:
        day = 0
        while True:
            day += 1
            logging.info(f"{'='*20} Day {day} {'='*20}")
            await run_one_day(target_configs)
            logging.info(f"Day {day} complete — replanning.")
            await asyncio.sleep(60)
    finally:
        for lf in lock_files:
            remove_lock(lf)
        _remove_scheduler_lock()


def main():
    signal.signal(signal.SIGINT,  _cleanup_and_exit)
    signal.signal(signal.SIGTERM, _cleanup_and_exit)
    signal.signal(signal.SIGHUP,  _cleanup_and_exit)

    parser = ArgumentParser(description="GroundBIRD continuous multi-target scheduler")
    parser.add_argument("--config", default=join(_SCRIPT_DIR, "priority.yaml"), help="Path to config YAML")
    parser.add_argument("--list",   action="store_true",     help="List active locks and exit")
    args = parser.parse_args()

    if args.list:
        list_active_targets()
        return
        
    if os.path.exists(SCHEDULER_LOCK_FILE):
        pid = int(open(SCHEDULER_LOCK_FILE).read().strip())
        try:
            os.kill(pid, 0)
            print(f"already running (PID={pid})")
            sys.exit(0)
        except OSError:
            pass

    _write_scheduler_lock()

    if not os.path.exists(args.config):
        print(f"ERROR: Config file not found: {args.config}")
        sys.exit(1)

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    target_configs = sorted(cfg.get("targets", []), key=lambda t: t["priority"])
    report_cfg     = cfg.get("report", None)

    if not target_configs:
        print("ERROR: No targets defined in config.")
        sys.exit(1)

    logging.info("GroundBIRD Continuous Scheduler starting")
    logging.info("Targets: " + "  >  ".join(
        f"{t['name']}({t['priority']})" for t in target_configs
    ))

    asyncio.run(run_continuous(target_configs, report_cfg))


if __name__ == "__main__":
    main()
