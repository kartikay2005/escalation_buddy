"""
Google Sheets integration module for storing and managing escalations.

This module handles reading from and writing to Google Sheets using gspread,
providing persistence for escalation events and their status with retry logic
for handling rate limits.

When Google credentials are not configured, uses an in-memory demo store
with sample data for demonstration purposes.
"""

import gspread
import logging
import os
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from functools import wraps
from google.oauth2.service_account import Credentials

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Column headers for the escalation sheet
HEADERS = [
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
    "Raw Body"
]

# Global worksheet client
_worksheet = None

# Demo mode flag and in-memory store
_demo_mode = False
_demo_data: List[Dict[str, Any]] = []


def _generate_demo_data() -> List[Dict[str, Any]]:
    """Generate realistic demo escalation data."""
    now = datetime.now(timezone.utc)

    return [
        {
            "ID": "esc-001-p1-legal",
            "Timestamp": (now - timedelta(hours=2)).isoformat(),
            "Source": "gmail",
            "Sender": "cfo@megacorp.com",
            "Account": "MegaCorp Industries",
            "Issue Type": "claim",
            "Priority": "P1",
            "Summary": "Legal action threatened - claim denial dispute",
            "Action Needed": "escalate",
            "Suggested Owner": "Legal Team",
            "Owner": "",
            "Status": "Open",
            "TAT Hours": 2,
            "Sentiment": "critical",
            "Raw Body": "Our legal team is preparing to file a lawsuit regarding the denied claim..."
        },
        {
            "ID": "esc-002-p1-medical",
            "Timestamp": (now - timedelta(hours=4)).isoformat(),
            "Source": "slack",
            "Sender": "hr@techstartup.io",
            "Account": "TechStartup Inc",
            "Issue Type": "claim",
            "Priority": "P1",
            "Summary": "Medical emergency - surgery pre-auth stuck",
            "Action Needed": "unblock",
            "Suggested Owner": "Claims Team",
            "Owner": "Sarah Chen",
            "Status": "In Progress",
            "TAT Hours": 4,
            "Sentiment": "critical",
            "Raw Body": "Employee needs emergency surgery, pre-authorization pending for 3 days..."
        },
        {
            "ID": "esc-003-p1-cancel",
            "Timestamp": (now - timedelta(hours=6)).isoformat(),
            "Source": "gmail",
            "Sender": "procurement@enterprise.co",
            "Account": "Enterprise Solutions",
            "Issue Type": "other",
            "Priority": "P1",
            "Summary": "Contract termination notice - service failures",
            "Action Needed": "escalate",
            "Suggested Owner": "Account Management",
            "Owner": "",
            "Status": "Open",
            "TAT Hours": 6,
            "Sentiment": "critical",
            "Raw Body": "Due to repeated issues, we are terminating our contract effective immediately..."
        },
        {
            "ID": "esc-004-p2-portal",
            "Timestamp": (now - timedelta(days=1)).isoformat(),
            "Source": "slack",
            "Sender": "admin@acmecorp.com",
            "Account": "Acme Corporation",
            "Issue Type": "technical",
            "Priority": "P2",
            "Summary": "Portal crashes when submitting claims",
            "Action Needed": "unblock",
            "Suggested Owner": "Tech Support",
            "Owner": "Mike Johnson",
            "Status": "In Progress",
            "TAT Hours": 24,
            "Sentiment": "frustrated",
            "Raw Body": "The claims portal has been crashing for 3 days. Error 500 on submit..."
        },
        {
            "ID": "esc-005-p2-billing",
            "Timestamp": (now - timedelta(days=1, hours=5)).isoformat(),
            "Source": "gmail",
            "Sender": "finance@globaltech.com",
            "Account": "GlobalTech Ltd",
            "Issue Type": "billing",
            "Priority": "P2",
            "Summary": "Duplicate charges on March invoice",
            "Action Needed": "followup",
            "Suggested Owner": "Billing Team",
            "Owner": "",
            "Status": "Open",
            "TAT Hours": 29,
            "Sentiment": "frustrated",
            "Raw Body": "We found duplicate charges totaling $15,000 on our latest invoice..."
        },
        {
            "ID": "esc-006-p2-onboard",
            "Timestamp": (now - timedelta(days=2)).isoformat(),
            "Source": "gmail",
            "Sender": "hr@newclient.org",
            "Account": "NewClient Organization",
            "Issue Type": "onboarding",
            "Priority": "P2",
            "Summary": "Employee onboarding stuck for 2 weeks",
            "Action Needed": "unblock",
            "Suggested Owner": "Onboarding Team",
            "Owner": "Lisa Park",
            "Status": "In Progress",
            "TAT Hours": 48,
            "Sentiment": "frustrated",
            "Raw Body": "50 employees have been waiting for enrollment completion..."
        },
        {
            "ID": "esc-007-p3-question",
            "Timestamp": (now - timedelta(days=3)).isoformat(),
            "Source": "slack",
            "Sender": "benefits@smallbiz.co",
            "Account": "SmallBiz Company",
            "Issue Type": "other",
            "Priority": "P3",
            "Summary": "Coverage limits clarification needed",
            "Action Needed": "info",
            "Suggested Owner": "Support Team",
            "Owner": "",
            "Status": "Open",
            "TAT Hours": 72,
            "Sentiment": "neutral",
            "Raw Body": "Quick question about outpatient procedure coverage limits..."
        },
        {
            "ID": "esc-008-p3-renewal",
            "Timestamp": (now - timedelta(days=4)).isoformat(),
            "Source": "gmail",
            "Sender": "admin@steadycorp.com",
            "Account": "SteadyCorp Inc",
            "Issue Type": "renewal",
            "Priority": "P3",
            "Summary": "Q2 renewal review scheduling",
            "Action Needed": "followup",
            "Suggested Owner": "Account Management",
            "Owner": "John Smith",
            "Status": "In Progress",
            "TAT Hours": 96,
            "Sentiment": "neutral",
            "Raw Body": "Our policy renews next quarter, would like to schedule a review..."
        },
        {
            "ID": "esc-009-p2-api",
            "Timestamp": (now - timedelta(hours=8)).isoformat(),
            "Source": "slack",
            "Sender": "devops@integration.io",
            "Account": "Integration Partners",
            "Issue Type": "technical",
            "Priority": "P2",
            "Summary": "API returning 500 errors - integration down",
            "Action Needed": "unblock",
            "Suggested Owner": "Tech Support",
            "Owner": "",
            "Status": "Open",
            "TAT Hours": 8,
            "Sentiment": "frustrated",
            "Raw Body": "Our HR sync integration is completely broken, API errors..."
        },
        {
            "ID": "esc-010-p3-feedback",
            "Timestamp": (now - timedelta(days=1)).isoformat(),
            "Source": "gmail",
            "Sender": "hr@happyclient.com",
            "Account": "Happy Client LLC",
            "Issue Type": "claim",
            "Priority": "P3",
            "Summary": "Positive feedback on recent claim handling",
            "Action Needed": "info",
            "Suggested Owner": "Support Team",
            "Owner": "",
            "Status": "Closed",
            "TAT Hours": 4,
            "Sentiment": "neutral",
            "Raw Body": "Just wanted to say thank you for the quick claim turnaround..."
        },
        {
            "ID": "esc-011-p2-delay",
            "Timestamp": (now - timedelta(hours=12)).isoformat(),
            "Source": "gmail",
            "Sender": "ops@fastgrow.com",
            "Account": "FastGrow Startup",
            "Issue Type": "claim",
            "Priority": "P2",
            "Summary": "Claim processing delayed 2 weeks",
            "Action Needed": "followup",
            "Suggested Owner": "Claims Team",
            "Owner": "Amy Wilson",
            "Status": "In Progress",
            "TAT Hours": 12,
            "Sentiment": "frustrated",
            "Raw Body": "Our claim submitted 2 weeks ago is still showing pending..."
        },
        {
            "ID": "esc-012-p3-docs",
            "Timestamp": (now - timedelta(days=2)).isoformat(),
            "Source": "slack",
            "Sender": "admin@reliableco.com",
            "Account": "ReliableCo",
            "Issue Type": "other",
            "Priority": "P3",
            "Summary": "Request for updated policy documents",
            "Action Needed": "info",
            "Suggested Owner": "Support Team",
            "Owner": "Tom Brown",
            "Status": "Resolved",
            "TAT Hours": 8,
            "Sentiment": "neutral",
            "Raw Body": "Can you send us the latest policy documentation?"
        },
    ]


def _init_demo_mode():
    """Initialize demo mode with sample data."""
    global _demo_mode, _demo_data
    _demo_mode = True
    _demo_data = _generate_demo_data()
    logger.info(f"Demo mode initialized with {len(_demo_data)} sample escalations")


def retry_on_rate_limit(max_attempts: int = 3, delay: float = 2.0):
    """
    Decorator to retry operations on rate limit errors.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Delay in seconds between retries
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    if "RATE_LIMIT" in str(e) or "quota" in str(e).lower():
                        last_error = e
                        if attempt < max_attempts - 1:
                            logger.warning(
                                f"Rate limit hit on {func.__name__}. Retrying in {delay}s "
                                f"(attempt {attempt + 1}/{max_attempts})"
                            )
                            time.sleep(delay)
                        continue
                    else:
                        raise
                except Exception as e:
                    # Don't retry on non-rate-limit errors
                    raise
            
            # If we got here, all retries exhausted
            logger.error(f"Rate limit retry exhausted for {func.__name__} after {max_attempts} attempts")
            raise last_error
        
        return wrapper
    return decorator


def init_sheet() -> Optional[gspread.Worksheet]:
    """
    Initialize Google Sheets connection and ensure header row exists.

    Authenticates using service account credentials from GOOGLE_CREDENTIALS_PATH
    env variable, opens the sheet by GOOGLE_SHEET_ID, and creates header row
    if it doesn't exist.

    If credentials are not configured, automatically falls back to demo mode
    with sample data.

    Returns:
        gspread.Worksheet: The opened worksheet, or None if in demo mode

    Raises:
        Exception: If authentication or sheet opening fails (not for missing config)
    """
    global _worksheet, _demo_mode

    try:
        # Get configuration from environment
        credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        sheet_name = os.getenv("SHEET_NAME", "Escalations")

        if not credentials_path or not sheet_id:
            logger.info("Google credentials not configured, using demo mode")
            _init_demo_mode()
            return None

        # Verify credentials file exists
        if not os.path.exists(credentials_path):
            logger.warning(f"Credentials file not found: {credentials_path}, using demo mode")
            _init_demo_mode()
            return None

        logger.info(f"Authenticating with Google Sheets using {credentials_path}")

        # Authenticate using service account
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        client = gspread.authorize(creds)

        # Open the spreadsheet
        logger.info(f"Opening spreadsheet {sheet_id}")
        spreadsheet = client.open_by_key(sheet_id)

        # Open or create the worksheet
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"Opened existing worksheet: {sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Worksheet '{sheet_name}' not found, creating...")
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(HEADERS))

        # Check if headers exist
        first_row = worksheet.row_values(1)

        if not first_row:
            # No data at all, insert header row
            logger.info("Creating header row")
            worksheet.insert_row(HEADERS, index=1)
        elif first_row != HEADERS:
            # Headers exist but don't match - update in place to avoid corrupting data
            logger.info("Updating header row to match expected columns")
            worksheet.update('A1', [HEADERS])
        else:
            logger.info("Header row already exists")

        _worksheet = worksheet
        _demo_mode = False
        logger.info("Sheet initialization complete")
        return worksheet

    except Exception as e:
        logger.warning(f"Failed to initialize Google Sheets: {str(e)}, using demo mode")
        _init_demo_mode()
        return None


@retry_on_rate_limit(max_attempts=3, delay=2.0)
def save_to_sheet(data: Dict[str, Any]) -> None:
    """
    Append a new escalation row to the sheet.

    Maps data fields to the header columns and appends a new row.
    Automatically sets Status="Open" and TAT Hours=0 if not provided.

    In demo mode, adds to in-memory store instead.

    Args:
        data: Escalation data dict with keys from process_escalation():
            - escalation_id: Unique ID for tracking
            - source: Where the escalation came from
            - sender: Who reported it
            - processed_at: When it was processed
            - subject: Subject line
            - body: Full message body
            - triage: Dict with AI analysis (priority, issue_type, etc.)

    Raises:
        Exception: If sheet operation fails (not in demo mode)
    """
    global _worksheet, _demo_mode, _demo_data

    # Initialize if needed
    if _worksheet is None and not _demo_mode:
        init_sheet()

    # Extract triage data
    triage = data.get("triage", {})

    # Create record dict
    record = {
        "ID": data.get("escalation_id", ""),
        "Timestamp": data.get("processed_at", datetime.now(timezone.utc).isoformat()),
        "Source": data.get("source", ""),
        "Sender": data.get("sender", ""),
        "Account": triage.get("account_name", "Unknown"),
        "Issue Type": triage.get("issue_type", "other"),
        "Priority": triage.get("priority", "P3"),
        "Summary": triage.get("summary", data.get("subject", "")),
        "Action Needed": triage.get("action_needed", "info"),
        "Suggested Owner": triage.get("suggested_owner", ""),
        "Owner": "",
        "Status": "Open",
        "TAT Hours": 0,
        "Sentiment": triage.get("sentiment", "neutral"),
        "Raw Body": data.get("body", "")[:500]
    }

    if _demo_mode:
        _demo_data.insert(0, record)  # Add to beginning (most recent)
        logger.info(f"[DEMO] Saved escalation {record['ID']} to in-memory store")
        return

    try:
        # Map data to columns in order
        row_data = [record[h] for h in HEADERS]

        logger.info(f"Appending row for escalation {data.get('escalation_id')}")
        _worksheet.append_row(row_data)
        logger.info(f"Successfully saved escalation {data.get('escalation_id')} to sheet")

    except Exception as e:
        logger.error(f"Error saving to sheet: {str(e)}")
        raise


@retry_on_rate_limit(max_attempts=3, delay=2.0)
def get_all_escalations() -> List[Dict[str, Any]]:
    """
    Retrieve all escalation records from the sheet.

    In demo mode, returns sample data from in-memory store.

    Returns:
        list: List of dictionaries, each representing an escalation record

    Raises:
        Exception: If sheet operation fails (not in demo mode)
    """
    global _worksheet, _demo_mode, _demo_data

    # Initialize if needed
    if _worksheet is None and not _demo_mode:
        init_sheet()

    if _demo_mode:
        logger.info(f"[DEMO] Returning {len(_demo_data)} escalations from in-memory store")
        return _demo_data.copy()

    try:
        logger.info("Retrieving all escalations from sheet")
        records = _worksheet.get_all_records()
        logger.info(f"Retrieved {len(records)} escalation records")
        return records

    except Exception as e:
        logger.error(f"Error retrieving escalations: {str(e)}")
        raise


@retry_on_rate_limit(max_attempts=3, delay=2.0)
def update_status(row_id: str, status: str, owner: str = "") -> bool:
    """
    Update the Status and Owner of an escalation by ID.

    Finds the row with matching ID and updates the Status and Owner columns.
    In demo mode, updates in-memory store.

    Args:
        row_id: The escalation ID to find and update
        status: New status value (e.g., "Open", "In Progress", "Closed", "Resolved")
        owner: Name or email of person owning the ticket (optional)

    Returns:
        bool: True if update was successful, False if row not found or invalid ID

    Raises:
        Exception: If sheet operation fails (not in demo mode)
    """
    global _worksheet, _demo_mode, _demo_data

    # Validate row_id
    if not row_id or not str(row_id).strip():
        logger.warning("update_status called with empty or invalid row_id")
        return False

    row_id = str(row_id).strip()

    # Initialize if needed
    if _worksheet is None and not _demo_mode:
        init_sheet()

    if _demo_mode:
        for record in _demo_data:
            if record.get("ID") == row_id:
                record["Status"] = status
                if owner:
                    record["Owner"] = owner
                logger.info(f"[DEMO] Updated escalation {row_id}: status={status}, owner={owner}")
                return True
        logger.warning(f"[DEMO] Escalation {row_id} not found")
        return False

    try:
        logger.info(f"Updating status for escalation {row_id} to '{status}'")

        # Get all records to find the row by ID
        records = _worksheet.get_all_records()

        # Find the row index (add 2 because get_all_records skips header, indices are 1-based)
        for idx, record in enumerate(records, start=2):
            if record.get("ID") == row_id:
                # Update Status column (index 12, 1-based)
                _worksheet.update_cell(idx, 12, status)

                # Update Owner column (index 11, 1-based) if provided
                if owner:
                    _worksheet.update_cell(idx, 11, owner)

                logger.info(f"Successfully updated escalation {row_id}: status={status}, owner={owner}")
                return True

        logger.warning(f"Escalation with ID {row_id} not found in sheet")
        return False

    except Exception as e:
        logger.error(f"Error updating status: {str(e)}")
        raise


@retry_on_rate_limit(max_attempts=3, delay=2.0)
def get_open_p1() -> List[Dict[str, Any]]:
    """
    Retrieve all open P1 priority escalations.

    Filters for records where Status not in ("Closed", "Resolved") and Priority == "P1".
    In demo mode, filters from in-memory store.

    Returns:
        list: List of open P1 escalation records

    Raises:
        Exception: If sheet operation fails (not in demo mode)
    """
    global _worksheet, _demo_mode, _demo_data

    # Initialize if needed
    if _worksheet is None and not _demo_mode:
        init_sheet()

    if _demo_mode:
        open_p1s = [
            record for record in _demo_data
            if str(record.get("Priority", "")).upper() == "P1"
            and str(record.get("Status", "")).upper() not in ("CLOSED", "RESOLVED")
        ]
        logger.info(f"[DEMO] Found {len(open_p1s)} open P1 escalations")
        return open_p1s

    try:
        logger.info("Retrieving open P1 escalations")
        records = _worksheet.get_all_records()

        # Filter for open P1s (case-insensitive)
        open_p1s = [
            record for record in records
            if str(record.get("Priority", "")).upper() == "P1"
            and str(record.get("Status", "")).upper() not in ("CLOSED", "RESOLVED")
        ]

        logger.info(f"Found {len(open_p1s)} open P1 escalations")
        return open_p1s

    except Exception as e:
        logger.error(f"Error retrieving open P1s: {str(e)}")
        raise


def is_demo_mode() -> bool:
    """Check if running in demo mode."""
    return _demo_mode


def initialize_sheets(sheet_id: str = None, sheet_name: str = None, 
                     credentials_path: str = None) -> gspread.Worksheet:
    """
    Initialize sheets with optional override parameters.
    
    This is a convenience wrapper around init_sheet() for backward compatibility.
    Uses environment variables by default, but allows parameter overrides.
    
    Args:
        sheet_id: Override GOOGLE_SHEET_ID env var
        sheet_name: Override SHEET_NAME env var
        credentials_path: Override GOOGLE_CREDENTIALS_PATH env var
        
    Returns:
        gspread.Worksheet: The initialized worksheet
    """
    # Set environment variables if overrides provided
    if sheet_id:
        os.environ["GOOGLE_SHEET_ID"] = sheet_id
    if sheet_name:
        os.environ["SHEET_NAME"] = sheet_name
    if credentials_path:
        os.environ["GOOGLE_CREDENTIALS_PATH"] = credentials_path
    
    return init_sheet()


if __name__ == "__main__":
    # Example usage (requires setup)
    logger.info("Google Sheets module loaded")
    logger.info("Call init_sheet() to initialize connection")
    logger.info("Call save_to_sheet(data) to append escalations")
    logger.info("Call get_all_escalations() to retrieve all records")
    logger.info("Call get_open_p1() to get critical escalations")

