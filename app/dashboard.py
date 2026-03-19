"""Streamlit dashboard for VP-level escalation monitoring and triage."""

from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import requests
import streamlit as st

try:
    from .sheets import get_all_escalations, update_status, is_demo_mode, init_sheet
except ImportError:
    from sheets import get_all_escalations, update_status, is_demo_mode, init_sheet

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None


WEBHOOK_URL = "http://localhost:5000/webhook"
DEFAULT_OWNER_OPTIONS = [
    "Claims Team",
    "Onboarding Team",
    "Tech Support",
    "Account Manager",
]


def setup_page() -> None:
    """Configure page and inject dark-friendly styling."""
    st.set_page_config(
        page_title="Escalation Command Centre",
        page_icon="🚨",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
        <style>
          .metric-card {
            background: linear-gradient(145deg, #171a21 0%, #202636 100%);
            border: 1px solid rgba(255,255,255,0.12);
            border-radius: 14px;
            padding: 12px 14px;
            margin-bottom: 8px;
          }
          .critical-head {
            color: #ff4b4b;
            font-weight: 700;
            font-size: 1.1rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=60)
def load_escalations() -> List[Dict[str, Any]]:
    """Load all escalations from Google Sheets with 60-second caching."""
    return get_all_escalations()


def to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert records to normalized DataFrame sorted by newest timestamp."""
    if not records:
        return pd.DataFrame(columns=[
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
        ])

    df = pd.DataFrame(records)
    expected_cols = [
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
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""

    df["Timestamp_parsed"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True)
    df["TAT Hours"] = pd.to_numeric(df["TAT Hours"], errors="coerce").fillna(0)
    df = df.sort_values(by="Timestamp_parsed", ascending=False)
    return df


def render_sidebar() -> bool:
    """Render sidebar controls and manual escalation form; returns refresh flag."""
    st.sidebar.header("Command Panel")
    refresh_clicked = st.sidebar.button("Refresh data", use_container_width=True)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Manual Escalation")
    with st.sidebar.form("manual_escalation_form", clear_on_submit=True):
        source = st.selectbox("Source", ["gmail", "slack"], index=0)
        sender = st.text_input("Sender")
        subject = st.text_input("Subject")
        body = st.text_area("Body", height=140)
        submitted = st.form_submit_button("Submit")

    if submitted:
        if not sender.strip() or not subject.strip() or not body.strip():
            st.sidebar.error("Sender, Subject, and Body are required.")
        else:
            payload = {
                "source": source,
                "sender": sender.strip(),
                "subject": subject.strip(),
                "body": body.strip(),
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
            try:
                response = requests.post(WEBHOOK_URL, json=payload, timeout=15)
                if response.ok:
                    st.sidebar.success("Escalation submitted successfully.")
                    load_escalations.clear()
                else:
                    st.sidebar.error(f"Webhook failed: {response.status_code} {response.text}")
            except requests.RequestException as exc:
                st.sidebar.error(f"Could not reach webhook: {exc}")

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return refresh_clicked


def compute_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Compute top-level dashboard metrics."""
    if df.empty:
        return {"open_total": 0, "open_p1": 0, "avg_tat_open": 0.0, "closed_today": 0}

    status_upper = df["Status"].astype(str).str.strip().str.upper()
    is_open = ~status_upper.isin(["CLOSED", "RESOLVED"])
    is_p1 = df["Priority"].astype(str).str.upper() == "P1"

    # Use UTC-aware timestamp for today comparison
    now_utc = pd.Timestamp.now(tz="UTC")
    today = now_utc.date()

    # Handle cases where Timestamp_parsed might be NaT
    valid_timestamps = df["Timestamp_parsed"].notna()
    closed_today = (
        (status_upper == "CLOSED") &
        valid_timestamps &
        (df["Timestamp_parsed"].dt.date == today)
    ).sum()

    open_tat = df.loc[is_open, "TAT Hours"]
    avg_tat = float(open_tat.mean()) if not open_tat.empty and not open_tat.isna().all() else 0.0

    return {
        "open_total": int(is_open.sum()),
        "open_p1": int((is_open & is_p1).sum()),
        "avg_tat_open": avg_tat,
        "closed_today": int(closed_today),
    }


def render_metrics(metrics: Dict[str, Any]) -> None:
    """Render four top-row VP metrics."""
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Total Open", metrics["open_total"])
        st.markdown("</div>", unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        critical_label = "P1 (Critical)"
        if metrics["open_p1"] > 0:
            critical_label = ":red[P1 (Critical)]"
        st.metric(critical_label, metrics["open_p1"])
        st.markdown("</div>", unsafe_allow_html=True)
    with c3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Avg TAT Hours (Open)", f"{metrics['avg_tat_open']:.1f}")
        st.markdown("</div>", unsafe_allow_html=True)
    with c4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Closed Today", metrics["closed_today"])
        st.markdown("</div>", unsafe_allow_html=True)


def render_p1_alerts(df: pd.DataFrame) -> None:
    """Render expandable P1 alert cards with ownership action."""
    st.markdown('<p class="critical-head">🔴 Critical — Requires Immediate Action</p>', unsafe_allow_html=True)

    if df.empty:
        st.info("No escalations available.")
        return

    status_upper = df["Status"].astype(str).str.strip().str.upper()
    is_open = ~status_upper.isin(["CLOSED", "RESOLVED"])
    is_p1 = df["Priority"].astype(str).str.upper() == "P1"
    p1_open = df[is_p1 & is_open]

    if p1_open.empty:
        st.success("No open P1 escalations right now.")
        return

    for _, row in p1_open.iterrows():
        row_id = str(row.get("ID", ""))
        account = str(row.get("Account", "Unknown") or "Unknown")
        summary = str(row.get("Summary", "(No summary)") or "(No summary)")
        exp_label = f"{account} | {summary[:80]}"
        with st.expander(exp_label, expanded=False):
            st.write(f"**Summary:** {summary}")
            st.write(f"**Account:** {account}")
            st.write(f"**Action Needed:** {row.get('Action Needed', '')}")
            st.write(f"**Source:** {row.get('Source', '')}")
            st.write(f"**Sender:** {row.get('Sender', '')}")
            st.write(f"**Suggested Owner:** {row.get('Suggested Owner', '')}")

            suggested = str(row.get("Suggested Owner", "") or "")
            owner_choices = [o for o in DEFAULT_OWNER_OPTIONS]
            if suggested and suggested not in owner_choices:
                owner_choices.insert(0, suggested)

            owner_key = f"owner_select_{row_id}"
            button_key = f"mark_in_progress_{row_id}"
            selected_owner = st.selectbox("Set Owner", owner_choices, key=owner_key)

            if st.button("Mark In Progress", key=button_key):
                if not row_id:
                    st.error("Missing escalation ID; cannot update status.")
                else:
                    updated = update_status(row_id=row_id, status="In Progress", owner=selected_owner)
                    if updated:
                        st.success(f"Escalation {row_id} marked In Progress.")
                        load_escalations.clear()
                    else:
                        st.error(f"Escalation {row_id} not found.")


def _priority_style(priority: Any) -> str:
    """Return style for priority highlighting."""
    p = str(priority).upper()
    if p == "P1":
        return "color: #ff4b4b; font-weight: 700"
    if p == "P2":
        return "color: #ffa725; font-weight: 700"
    if p == "P3":
        return "color: #2ecc71; font-weight: 700"
    return ""


def render_full_table(df: pd.DataFrame) -> None:
    """Render filterable escalation table with priority color coding."""
    st.subheader("All Escalations")

    if df.empty:
        st.info("No escalation records found.")
        return

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        pr_options = sorted([x for x in df["Priority"].dropna().astype(str).unique() if x])
        pr_sel = st.multiselect("Priority", options=pr_options, default=pr_options)
    with f2:
        st_options = sorted([x for x in df["Status"].dropna().astype(str).unique() if x])
        st_sel = st.multiselect("Status", options=st_options, default=st_options)
    with f3:
        issue_options = sorted([x for x in df["Issue Type"].dropna().astype(str).unique() if x])
        issue_sel = st.multiselect("Issue Type", options=issue_options, default=issue_options)
    with f4:
        src_options = sorted([x for x in df["Source"].dropna().astype(str).unique() if x])
        src_sel = st.multiselect("Source", options=src_options, default=src_options)

    # Handle empty filter selections - show nothing if user deselects all
    if not pr_sel or not st_sel or not issue_sel or not src_sel:
        st.warning("Please select at least one option in each filter to view escalations.")
        return

    filtered = df[
        df["Priority"].astype(str).isin(pr_sel)
        & df["Status"].astype(str).isin(st_sel)
        & df["Issue Type"].astype(str).isin(issue_sel)
        & df["Source"].astype(str).isin(src_sel)
    ].copy()

    if filtered.empty:
        st.info("No escalations match the selected filters.")
        return

    filtered = filtered.sort_values(by="Timestamp_parsed", ascending=False)

    display_cols = [
        "Timestamp",
        "Priority",
        "Status",
        "Issue Type",
        "Account",
        "Summary",
        "Source",
        "Sender",
        "Owner",
        "TAT Hours",
    ]
    display_df = filtered[display_cols]
    styled = display_df.style.map(_priority_style, subset=["Priority"])
    st.dataframe(styled, use_container_width=True)


def main() -> None:
    """Render the Escalation Command Centre dashboard."""
    setup_page()

    # Initialize sheets (will use demo mode if credentials not configured)
    init_sheet()

    if st_autorefresh is not None:
        st_autorefresh(interval=60_000, key="vp_dashboard_refresh")

    refresh_clicked = render_sidebar()
    if refresh_clicked:
        load_escalations.clear()

    st.title("Escalation Command Centre")
    st.caption("VP View - Account Management, Critical Risk Tracking, and Ownership Control")

    try:
        records = load_escalations()
        df = to_dataframe(records)
    except Exception as exc:
        st.error(f"Failed to load escalation data: {exc}")
        return

    metrics = compute_metrics(df)
    render_metrics(metrics)

    st.markdown("---")
    render_p1_alerts(df)

    st.markdown("---")
    render_full_table(df)


if __name__ == "__main__":
    main()
