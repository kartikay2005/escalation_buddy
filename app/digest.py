"""Daily escalation digest generator.

This script can run standalone on a schedule (cron/Task Scheduler) and produces
an executive Slack digest from Google Sheets escalation data.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv

try:
    from .sheets import get_all_escalations, get_open_p1
except ImportError:
    from sheets import get_all_escalations, get_open_p1


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _to_df(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert raw sheet records into a normalized DataFrame."""
    if not records:
        return pd.DataFrame(
            columns=[
                "ID",
                "Timestamp",
                "Source",
                "Sender",
                "Account",
                "Issue Type",
                "Priority",
                "Summary",
                "Action Needed",
                "Suggested Owner",
                "Owner",
                "Status",
                "TAT Hours",
                "Sentiment",
                "Raw Body",
            ]
        )

    df = pd.DataFrame(records)
    for col in [
        "ID",
        "Timestamp",
        "Source",
        "Sender",
        "Account",
        "Issue Type",
        "Priority",
        "Summary",
        "Action Needed",
        "Suggested Owner",
        "Owner",
        "Status",
        "TAT Hours",
        "Sentiment",
        "Raw Body",
    ]:
        if col not in df.columns:
            df[col] = ""

    df["TimestampParsed"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True)
    return df


def _normalize_open(df: pd.DataFrame) -> pd.DataFrame:
    """Return only open rows (status != Closed)."""
    if df.empty:
        return df
    status_upper = df["Status"].astype(str).str.strip().str.upper()
    return df[status_upper != "CLOSED"].copy()


def _format_p1_lines(p1_open_df: pd.DataFrame) -> str:
    """Format P1 rows for Slack section text."""
    if p1_open_df.empty:
        return "- None"

    lines: List[str] = []
    for _, row in p1_open_df.iterrows():
        account = str(row.get("Account", "Unknown") or "Unknown")
        summary = str(row.get("Summary", "(No summary)") or "(No summary)")
        action = str(row.get("Action Needed", "followup") or "followup")
        lines.append(f"- *{account}*: {summary} (Action: *{action}*)")
    return "\n".join(lines)


def _format_p2_oldest_lines(p2_open_df: pd.DataFrame) -> str:
    """Format top 3 oldest open P2 rows for Slack section text."""
    if p2_open_df.empty:
        return "- None"

    p2_oldest = p2_open_df.sort_values(by="TimestampParsed", ascending=True).head(3)
    lines: List[str] = []
    for _, row in p2_oldest.iterrows():
        account = str(row.get("Account", "Unknown") or "Unknown")
        summary = str(row.get("Summary", "(No summary)") or "(No summary)")
        ts = row.get("TimestampParsed")
        ts_text = ts.strftime("%Y-%m-%d") if pd.notnull(ts) else "Unknown date"
        lines.append(f"- *{account}* ({ts_text}): {summary}")
    return "\n".join(lines)


def build_digest_payload(
    all_escalations: List[Dict[str, Any]],
    open_p1_rows: List[Dict[str, Any]],
    dashboard_url: str,
) -> Dict[str, Any]:
    """Build Slack Block Kit payload for daily briefing."""
    today_text = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    all_df = _to_df(all_escalations)
    open_df = _normalize_open(all_df)

    p1_open_df = _to_df(open_p1_rows)
    p1_open_df = _normalize_open(p1_open_df)

    if open_df.empty:
        p2_open_df = open_df
    else:
        p2_open_df = open_df[open_df["Priority"].astype(str).str.upper() == "P2"].copy()

    closed_today_count = 0
    if not all_df.empty:
        today = datetime.now(timezone.utc).date()
        status_upper = all_df["Status"].astype(str).str.strip().str.upper()
        closed_today = all_df[(status_upper == "CLOSED") & (all_df["TimestampParsed"].dt.date == today)]
        closed_today_count = len(closed_today)

    p1_count = len(p1_open_df)
    p2_count = len(p2_open_df)

    p1_lines = _format_p1_lines(p1_open_df)
    p2_lines = _format_p2_oldest_lines(p2_open_df)

    payload = {
        "text": f"📋 Daily Escalation Briefing — {today_text}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📋 Daily Escalation Briefing — {today_text}",
                    "emoji": True,
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔴 P1 Open: {p1_count}*\n{p1_lines}",
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🟡 P2 Open: {p2_count}*\n{p2_lines}",
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*✅ Closed today: {closed_today_count}*",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Dashboard: <{dashboard_url}|Open Streamlit Dashboard>",
                    }
                ],
            },
        ],
    }
    return payload


def send_to_slack(payload: Dict[str, Any], webhook_url: str) -> None:
    """Post payload to Slack incoming webhook."""
    response = requests.post(webhook_url, json=payload, timeout=20)
    response.raise_for_status()


def run_dry_run(payload: Dict[str, Any]) -> None:
    """Print Slack payload to console without sending."""
    print(json.dumps(payload, indent=2))


def main() -> int:
    """CLI entry point for daily digest generation and posting."""
    parser = argparse.ArgumentParser(description="Generate and send daily escalation digest to Slack")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the Slack payload to console without sending",
    )
    args = parser.parse_args()

    dashboard_url = os.getenv("STREAMLIT_DASHBOARD_URL", "http://localhost:8501")
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()

    try:
        all_escalations = get_all_escalations()
        open_p1_rows = get_open_p1()
    except Exception as exc:
        logger.error("Failed to load escalation data from sheets: %s", exc)
        all_escalations = []
        open_p1_rows = []

    payload = build_digest_payload(
        all_escalations=all_escalations,
        open_p1_rows=open_p1_rows,
        dashboard_url=dashboard_url,
    )

    if args.dry_run:
        run_dry_run(payload)
        logger.info("Dry run complete. Payload printed without sending.")
        return 0

    if not slack_webhook_url:
        logger.error("SLACK_WEBHOOK_URL is not set. Use --dry-run to preview payload.")
        return 1

    try:
        send_to_slack(payload, slack_webhook_url)
        logger.info("Daily digest posted to Slack successfully.")
        return 0
    except requests.RequestException as exc:
        logger.error("Failed to post digest to Slack: %s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
