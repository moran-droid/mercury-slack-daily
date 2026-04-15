#!/usr/bin/env python3
"""
Mercury -> Slack daily balance report.

Posts a DM with total balance and yesterday's cash in / cash out / net P&L.
Credit-card payoff transfers from checking are expanded inline into the
individual charges they covered (charges since the previous payoff).
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

CREDIT_PAYOFF_KEYWORDS = ("mercury credit", "io credit", "credit card payment", "cc payment")
VALID_STATUSES = {"sent", "posted"}


def is_credit_account(acct: dict) -> bool:
    for key in ("kind", "type", "accountType"):
        val = acct.get(key)
        if isinstance(val, str) and "credit" in val.lower():
            return True
    name = (acct.get("name") or "").lower()
    return "credit" in name


def looks_like_credit_payoff(txn: dict) -> bool:
    for key in ("counterpartyNickname", "counterpartyName", "bankDescription", "note"):
        val = txn.get(key)
        if isinstance(val, str):
            low = val.lower()
            if any(kw in low for kw in CREDIT_PAYOFF_KEYWORDS):
                return True
    return False


def txn_ts(txn: dict) -> datetime | None:
    ts_str = txn.get("postedAt") or txn.get("createdAt")
    if not ts_str:
        return None
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(TZ)


def txn_counterparty(txn: dict) -> str:
    return (
        txn.get("counterpartyNickname")
        or txn.get("counterpartyName")
        or txn.get("bankDescription")
        or txn.get("note")
        or "Unknown"
    )


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


def fetch_transactions(account_id: str, start_date, end_date) -> list[dict]:
    params = {
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
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
    return [t for t in txns if (t.get("status") or "").lower() in VALID_STATUSES]


def build_credit_payoff_map(credit_accounts: list[dict], day_start: datetime, day_end: datetime) -> dict:
    """
    For each yesterday payoff on a credit account, find the charges it covered
    (charges on that credit account since the previous payoff).

    Returns: { (credit_account_id, rounded_amount): [charge_txns...] }
    Amount is keyed on the absolute value so it matches the checking-side outflow.
    """
    lookback_start = (day_start - timedelta(days=60)).date()
    end_date = day_end.date()
    result: dict[tuple[str, float], list[dict]] = {}

    for acct in credit_accounts:
        txns = fetch_transactions(acct["id"], lookback_start, end_date)
        dated = []
        for t in txns:
            ts = txn_ts(t)
            if ts is None:
                continue
            dated.append((ts, t))
        dated.sort(key=lambda x: x[0])

        prev_payoff_idx = -1
        for i, (ts, t) in enumerate(dated):
            amount = float(t.get("amount", 0))
            # Positive amount on a credit account = payment from checking (payoff)
            if amount <= 0:
                continue
            if day_start <= ts < day_end:
                charges = []
                for j in range(prev_payoff_idx + 1, i):
                    cj = dated[j][1]
                    cj_amt = float(cj.get("amount", 0))
                    if cj_amt < 0:
                        charges.append(cj)
                key = (acct["id"], round(amount, 2))
                result[key] = charges
            prev_payoff_idx = i
    return result


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

    spending_accounts = [a for a in accounts if not is_credit_account(a)]
    credit_accounts = [a for a in accounts if is_credit_account(a)]

    # Precompute, per credit account, the charge lists keyed by yesterday payoff amount.
    payoff_map = build_credit_payoff_map(credit_accounts, day_start, day_end)
    # Flatten: amount -> charges (first match wins if multiple credit accounts share an amount)
    flat_payoff_map: dict[float, list[dict]] = {}
    for (_aid, amt), charges in payoff_map.items():
        flat_payoff_map.setdefault(amt, charges)

    cash_in = 0.0
    cash_out = 0.0
    outflows_by_counterparty: dict[str, float] = {}

    for acct in spending_accounts:
        for txn in fetch_transactions(acct["id"], yesterday, yesterday + timedelta(days=1)):
            ts = txn_ts(txn)
            if ts is None or not (day_start <= ts < day_end):
                continue
            amount = float(txn.get("amount", 0))
            if amount >= 0:
                cash_in += amount
                continue
            out_amt = -amount
            cash_out += out_amt
            if looks_like_credit_payoff(txn):
                charges = flat_payoff_map.get(round(out_amt, 2))
                if charges:
                    for c in charges:
                        c_amt = -float(c.get("amount", 0))
                        name = txn_counterparty(c)
                        outflows_by_counterparty[name] = outflows_by_counterparty.get(name, 0.0) + c_amt
                    continue
            name = txn_counterparty(txn)
            outflows_by_counterparty[name] = outflows_by_counterparty.get(name, 0.0) + out_amt

    net = round(cash_in - cash_out, 2)
    cash_in = round(cash_in, 2)
    cash_out = round(cash_out, 2)

    top_outflows = sorted(outflows_by_counterparty.items(), key=lambda kv: kv[1], reverse=True)[:10]
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
