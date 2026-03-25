#!/usr/bin/env python3
"""
mailer.py — Schedule email report for GroundBIRD continuous scheduler

Uses logger_base.mail_sender directly (same as the existing mailsender.py),
but without the telescope status or interactive confirmation.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from astropy.time import Time

from logger_base.mail_sender import make_message, send_via_gmail
from planner import get_plan_oneday

FROM_ADDR = "gbird.auto@gmail.com"


# ═════════════════════════════════════════════════════════════════════════════
# Email sending
# ═════════════════════════════════════════════════════════════════════════════

def send_mail(to_addr: list[str], subject: str, body: str):
    m = make_message(FROM_ADDR, to_addr, subject, body)
    send_via_gmail(m)
    logging.info(f"[MAIL] Sent '{subject}' to {to_addr}")


# ═════════════════════════════════════════════════════════════════════════════
# Report body
# ═════════════════════════════════════════════════════════════════════════════

def build_report_body(target_configs: list[dict], days_ahead: int) -> str:
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=days_ahead)

    # ── Collect all reset events across the period ────────────────────────────
    # all_resets: list of (datetime, target_name, action_str)
    all_resets: list[tuple[datetime, str, str]] = []
    obs_count:  dict[str, int] = {t["name"]: 0 for t in target_configs}

    for day_offset in range(days_ahead):
        day_start = now + timedelta(days=day_offset)
        for tcfg in target_configs:
            table = get_plan_oneday(
                time=Time(day_start),
                line=tcfg["elevation"],
                body_name=tcfg["name"],
            )
            for _, row in table.iterrows():
                action: str = row["Action"]
                # Keep only reset (target) events, skip dome/sun events
                if "Restart GB" not in action:
                    continue
                evt_time = pd.to_datetime(row["Time (UTC)"])
                if evt_time.tzinfo is None:
                    evt_time = evt_time.replace(tzinfo=timezone.utc)
                all_resets.append((evt_time, tcfg["name"], action))
                # Count unique rise/set groups per target per day
                # (one observation = one rise or set crossing, 7 GB resets each)
                # We count each action line as one reset command sent
                obs_count[tcfg["name"]] += 1

    all_resets.sort(key=lambda x: x[0])

    # ── Build email body ──────────────────────────────────────────────────────
    lines = [
        "Dear Observers,",
        "",
        "Here is the GroundBIRD automated observation schedule.",
        "",
        f"  Period : {now.strftime('%Y-%m-%d %H:%M UTC')}  →  {end.strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]

    # Priority order
    lines += [
        "─" * 55,
        "  PRIORITY ORDER",
        "─" * 55,
    ]
    for tcfg in target_configs:
        lines.append(f"  {tcfg['priority']}.  {tcfg['name']}")
    lines.append("")

    # Observation count per target
    lines += [
        "─" * 55,
        "  RESET COMMANDS PER TARGET  (over the period)",
        "─" * 55,
    ]
    for tcfg in target_configs:
        count = obs_count[tcfg["name"]]
        lines.append(f"  {tcfg['name']:12s}  {count:4d} commands")
    lines.append("")

    # Chronological reset command schedule
    lines += [
        "─" * 55,
        "  RESET COMMAND SCHEDULE  (targets only, chronological)",
        "─" * 55,
    ]
    if all_resets:
        current_day = None
        for evt_time, tname, action in all_resets:
            day_str = evt_time.strftime("%Y-%m-%d (%a)")
            if day_str != current_day:
                if current_day is not None:
                    lines.append("")
                lines.append(f"  [ {day_str} ]")
                current_day = day_str
            lines.append(f"    {evt_time.strftime('%H:%M UTC')}  [{tname:10s}]  {action}")
    else:
        lines.append("  No reset commands scheduled for this period.")
    lines.append("")

    lines += [
        "─" * 55,
        "This message was generated automatically by continuous_scheduler.py.",
    ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# Email report loop (runs as background asyncio task)
# ═════════════════════════════════════════════════════════════════════════════

async def email_report_loop(report_cfg: dict, target_configs: list[dict]):
    to_addr    = report_cfg["to_addr"]
    weekdays   = report_cfg["weekdays"]
    report_h, report_m = map(int, report_cfg["time_utc"].split(":"))
    days_ahead = report_cfg["report_days_ahead"]

    while True:
        now = datetime.now(timezone.utc)

        # Find the next matching weekday/time within the coming 8 days
        next_report = next(
            (now + timedelta(days=delta)).replace(
                hour=report_h, minute=report_m, second=0, microsecond=0
            )
            for delta in range(1, 9)
            if (now + timedelta(days=delta)).weekday() in weekdays
        )

        wait = (next_report - now).total_seconds()
        logging.info(
            f"[MAIL] Next report: {next_report.strftime('%Y-%m-%d %H:%M UTC')} "
            f"(in {wait / 3600:.1f}h)"
        )
        await asyncio.sleep(wait)

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        end_str = (datetime.now(timezone.utc) + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        subject = f"GroundBIRD Observation Schedule  {now_str} ~ {end_str}"
        body    = build_report_body(target_configs, days_ahead)
        send_mail(to_addr, subject, body)
