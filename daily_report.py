#!/usr/bin/env python3
"""
Mercury -> Slack daily balance report.

Fetches current balances from Mercury, computes daily / MTD / YTD P&L
against a state file, and posts a DM to the configured Slack user.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

MERCURY_API = "https://api.mercury.com/api/v1"
SLACK_API = "https://slack.com/api"
STATE_FILE = Path(__file__).parent / "state.json"
TZ = ZoneInfo("Asia/Nicosia")

MERCURY_TOKEN = os.environ["MERCURY_TOKEN"]
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_USER_ID = os.environ["SLACK_USER_ID"]
RECIPIENT_NAME = os.environ.get("RECIPIENT_NAME", "Moran")

ACCOUNT_IDS_FILTER = {
    a.strip() for a in os.environ.get("MERCURY_ACCOUNT_IDS", "").split(",") if a.strip()
}


def fetch_total_balance() -> tuple[float, list[dict]]:
    r = requests.get(
        f"{MERCURY_API}/accounts",
        headers={"Authorization": f"Bearer secret-token:{MERCURY_TOKEN}"},
        timeout=30,
    )
    r.raise_for_status()
    payload = r.json()
    accounts = payload.get("accounts", payload if isinstance(payload, list) else [])
    included = []
    total = 0.0
    for acct in accounts:
        if acct.get("status") and acct["status"].lower() != "active":
            continue
        if ACCOUNT_IDS_FILTER and acct.get("id") not in ACCOUNT_IDS_FILTER:
            continue
        balance = acct.get("currentBalance")
        if balance is None:
            balance = acct.get("availableBalance", 0)
        total += float(balance)
        included.append(acct)
    if not included:
        raise RuntimeError("No matching Mercury accounts found.")
    return round(total, 2), included


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")


def fmt_money(n: float) -> str:
    return f"${n:,.2f}"


def fmt_delta(n: float) -> str:
    arrow = "↑" if n >= 0 else "↓"
    return f"{arrow} {fmt_money(abs(n))}"


MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]


def build_message(name, today, current, daily, mtd, ytd):
    month_label = f"{MONTHS[today.month - 1]} {today.year}"
    lines = [f"Hey {name},", "", f"Balance: {fmt_money(current)}", ""]
    if daily is not None:
        lines += [f"Daily P&L: {fmt_delta(daily)}", ""]
    if mtd is not None:
        lines += [f"MTD ({month_label}): {fmt_delta(mtd)}", ""]
    if ytd is not None:
        lines += [f"YTD ({today.year}): {fmt_delta(ytd)}"]
    return "\n".join(lines).rstrip()


def open_dm(user_id):
    r = requests.post(
        f"{SLACK_API}/conversations.open",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json={"users": user_id},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack conversations.open failed: {data}")
    return data["channel"]["id"]


def post_slack(channel, text):
    r = requests.post(
        f"{SLACK_API}/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json={"channel": channel, "text": text, "mrkdwn": True},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage failed: {data}")


def main():
    today = datetime.now(TZ)
    today_date = today.strftime("%Y-%m-%d")
    month_key = today.strftime("%Y-%m")
    year_key = str(today.year)
    current, included = fetch_total_balance()
    print(f"[{today_date}] current balance: {fmt_money(current)} across {len(included)} account(s)")
    state = load_state()
    is_new_day = state.get("today_snapshot_date") != today_date
    if is_new_day:
        prev_snapshot = state.get("today_snapshot")
        state["yesterday_balance"] = prev_snapshot
        state["yesterday_date"] = state.get("today_snapshot_date")
        state["today_snapshot"] = current
        state["today_snapshot_date"] = today_date
        if state.get("month_key") != month_key:
            state["month_start_balance"] = prev_snapshot if prev_snapshot is not None else current
            state["month_key"] = month_key
        if state.get("year_key") != year_key:
            state["year_start_balance"] = prev_snapshot if prev_snapshot is not None else current
            state["year_key"] = year_key
    prev_balance = state.get("yesterday_balance")
    first_run = prev_balance is None
    if first_run:
        message = (
            f"Hey {RECIPIENT_NAME},\n\n"
            f"Daily Mercury report is live.\n\n"
            f"Starting balance: {fmt_money(current)}\n\n"
            f"Tomorrow you'll start getting daily P&L updates."
        )
    else:
        daily = current - prev_balance
        mtd = current - state["month_start_balance"]
        ytd = current - state["year_start_balance"]
        message = build_message(RECIPIENT_NAME, today, current, daily, mtd, ytd)
    print("---- message ----")
    print(message)
    print("-----------------")
    channel = open_dm(SLACK_USER_ID)
    post_slack(channel, message)
    print("posted to slack")
    save_state(state)
    print(f"state saved -> {STATE_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
