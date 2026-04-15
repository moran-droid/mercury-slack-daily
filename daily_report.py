#!/usr/bin/env python3
"""
Mercury -> Slack daily balance report.

Posts a DM with total balance and today's cash in / cash out / net P&L,
computed from Mercury's transactions endpoint.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import requests

MERCURY_API = "https://api.mercury.com/api/v1"
SLACK_API = "https://slack.com/api"
TZ = ZoneInfo("Asia/Nicosia")

MERCURY_TOKEN = os.environ["MERCURY_TOKEN"]
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_USER_ID = os.environ["SLACK_USER_ID"]
RECIPIENT_NAME = os.environ.get("RECIPIENT_NAME", "Moran")

ACCOUNT_IDS_FILTER = {
    a.strip() for a in os.environ.get("MERCURY_ACCOUNT_IDS", "").split(",") if a.strip()
}

MERCURY_HEADERS = {"Authorization": f"Bearer secret-token:{MERCURY_TOKEN}"}
SLACK_HEADERS = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}


def fetch_accounts() -> list[dict]:
    r = requests.get(f"{MERCURY_API}/accounts", headers=MERCURY_HEADERS, timeout=30)
    r.raise_for_status()
    payload = r.json()
    accounts = payload.get("accounts", payload if isinstance(payload, list) else [])
    result = []
    for acct in accounts:
        if acct.get("status") and acct["status"].lower() != "active":
            continue
        if ACCOUNT_IDS_FILTER and acct.get("id") not in ACCOUNT_IDS_FILTER:
            continue
        result.append(acct)
    if not result:
        raise RuntimeError("No matching Mercury accounts found.")
    return result


def account_balance(acct: dict) -> float:
    balance = acct.get("currentBalance")
    if balance is None:
        balance = acct.get("availableBalance", 0)
    return float(balance)


def fetch_transactions_for_day(account_id: str, day_start: datetime, day_end: datetime) -> list[dict]:
    """Fetch transactions posted during [day_start, day_end) for one account."""
    params = {
        "start": day_start.date().isoformat(),
        "end": day_end.date().isoformat(),
        "limit": 500,
    }
    r = requests.get(
        f"{MERCURY_API}/account/{account_id}/transactions",
        headers=MERCURY_HEADERS,
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    txns = data.get("transactions", data if isinstance(data, list) else [])
    filtered = []
    for t in txns:
        if (t.get("status") or "").lower() != "sent":
            continue
        ts_str = t.get("postedAt") or t.get("createdAt")
        if not ts_str:
            continue
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(TZ)
        if day_start <= ts < day_end:
            filtered.append(t)
    return filtered


def fmt_money(n: float) -> str:
    return f"${n:,.2f}"


def fmt_delta(n: float) -> str:
    arrow = "↑" if n >= 0 else "↓"
    return f"{arrow} {fmt_money(abs(n))}"


def open_dm(user_id: str) -> str:
    r = requests.post(
        f"{SLACK_API}/conversations.open",
        headers=SLACK_HEADERS,
        json={"users": user_id},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack conversations.open failed: {data}")
    return data["channel"]["id"]


def post_slack(channel: str, text: str) -> None:
    r = requests.post(
        f"{SLACK_API}/chat.postMessage",
        headers=SLACK_HEADERS,
        json={"channel": channel, "text": text, "mrkdwn": True},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage failed: {data}")


def main() -> int:
    now = datetime.now(TZ)
    yesterday = (now - timedelta(days=1)).date()
    day_start = datetime.combine(yesterday, time.min, tzinfo=TZ)
    day_end = day_start + timedelta(days=1)

    accounts = fetch_accounts()
    total_balance = round(sum(account_balance(a) for a in accounts), 2)

    cash_in = 0.0
    cash_out = 0.0
    outflows_by_counterparty: dict[str, float] = {}
    for acct in accounts:
        for txn in fetch_transactions_for_day(acct["id"], day_start, day_end):
            amount = float(txn.get("amount", 0))
            if amount >= 0:
                cash_in += amount
            else:
                out_amt = -amount
                cash_out += out_amt
                name = (
                    txn.get("counterpartyNickname")
                    or txn.get("counterpartyName")
                    or txn.get("bankDescription")
                    or "Unknown"
                )
                outflows_by_counterparty[name] = outflows_by_counterparty.get(name, 0.0) + out_amt
    net = round(cash_in - cash_out, 2)
    cash_in = round(cash_in, 2)
    cash_out = round(cash_out, 2)

    top_outflows = sorted(outflows_by_counterparty.items(), key=lambda kv: kv[1], reverse=True)[:5]
    breakdown_line = ""
    if top_outflows:
        parts = [f"{name} {fmt_money(amt)}" for name, amt in top_outflows]
        breakdown_line = f" ({', '.join(parts)})"

    date_label = yesterday.strftime("%b %d, %Y")
    message = (
        f"Hey {RECIPIENT_NAME},\n\n"
        f"Total balance: {fmt_money(total_balance)}\n\n"
        f"Daily P&L ({date_label}): {fmt_delta(net)}\n"
        f"  Cash in:  {fmt_money(cash_in)}\n"
        f"  Cash out: {fmt_money(cash_out)}{breakdown_line}"
    )

    print("---- message ----")
    print(message)
    print("-----------------")

    channel = open_dm(SLACK_USER_ID)
    post_slack(channel, message)
    print("posted to slack")
    return 0


if __name__ == "__main__":
    sys.exit(main())
