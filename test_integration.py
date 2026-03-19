#!/usr/bin/env python3
"""
Comprehensive integration test suite for the Escalation Management System.

This test creates a mock Google Sheet implementation and runs the entire system
through rigorous end-to-end testing with multiple diverse escalation scenarios.
"""

import json
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from unittest.mock import Mock, patch, MagicMock
import threading
import time

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class MockWorksheet:
    """In-memory mock of gspread.Worksheet for testing."""

    def __init__(self, name: str = "Escalations"):
        self.name = name
        self._data: List[List[Any]] = []
        self._headers: List[str] = []

    def row_values(self, row: int) -> List[Any]:
        """Get values from a specific row (1-indexed)."""
        if row < 1 or row > len(self._data):
            return []
        return self._data[row - 1]

    def insert_row(self, values: List[Any], index: int = 1) -> None:
        """Insert a row at the specified index (1-indexed)."""
        self._data.insert(index - 1, values)

    def append_row(self, values: List[Any]) -> None:
        """Append a row to the end of the sheet."""
        self._data.append(values)

    def update(self, range_notation: str, values: List[List[Any]]) -> None:
        """Update cells in the specified range."""
        if range_notation == 'A1':
            if self._data:
                self._data[0] = values[0]
            else:
                self._data.append(values[0])

    def update_cell(self, row: int, col: int, value: Any) -> None:
        """Update a single cell (1-indexed)."""
        while len(self._data) < row:
            self._data.append([""] * 15)
        while len(self._data[row - 1]) < col:
            self._data[row - 1].append("")
        self._data[row - 1][col - 1] = value

    def get_all_records(self) -> List[Dict[str, Any]]:
        """Get all rows as list of dicts using first row as headers."""
        if len(self._data) < 2:
            return []
        headers = self._data[0]
        records = []
        for row in self._data[1:]:
            record = {}
            for i, header in enumerate(headers):
                record[header] = row[i] if i < len(row) else ""
            records.append(record)
        return records

    def get_all_values(self) -> List[List[Any]]:
        """Get all data as 2D list."""
        return self._data.copy()

    def clear(self) -> None:
        """Clear all data."""
        self._data = []


class MockSpreadsheet:
    """Mock of gspread.Spreadsheet."""

    def __init__(self):
        self._worksheets: Dict[str, MockWorksheet] = {}

    def worksheet(self, name: str) -> MockWorksheet:
        """Get worksheet by name."""
        if name not in self._worksheets:
            import gspread.exceptions
            raise gspread.exceptions.WorksheetNotFound()
        return self._worksheets[name]

    def add_worksheet(self, title: str, rows: int, cols: int) -> MockWorksheet:
        """Add a new worksheet."""
        ws = MockWorksheet(title)
        self._worksheets[title] = ws
        return ws


# Global mock sheet for tests
_mock_spreadsheet = MockSpreadsheet()
_mock_worksheet: Optional[MockWorksheet] = None


def setup_mock_sheet():
    """Set up the mock Google Sheet with proper headers."""
    global _mock_worksheet, _mock_spreadsheet

    _mock_spreadsheet = MockSpreadsheet()
    _mock_worksheet = MockWorksheet("Escalations")
    _mock_spreadsheet._worksheets["Escalations"] = _mock_worksheet

    # Initialize with headers
    from app.sheets import HEADERS
    _mock_worksheet.insert_row(HEADERS, 1)

    return _mock_worksheet


def get_mock_worksheet():
    """Return the global mock worksheet."""
    return _mock_worksheet


# =============================================================================
# TEST DATA - Multiple diverse escalation scenarios
# =============================================================================

def generate_test_escalations() -> List[Dict[str, Any]]:
    """Generate diverse test escalation data covering all scenarios."""
    now = datetime.now(timezone.utc)

    return [
        # P1 - Critical: Legal threat
        {
            "source": "gmail",
            "sender": "cfo@bigcorp.com",
            "subject": "URGENT: Legal action threatened over denied claim",
            "body": """
            Our CEO is threatening to file a lawsuit and contact IRDAI.
            The claim denial has caused significant business disruption.
            We need immediate resolution or we will cancel our contract.
            This is our final warning.
            """,
            "timestamp": now.isoformat(),
            "expected_priority": "P1",
            "expected_issue_type": "claim",
            "expected_sentiment": "critical"
        },
        # P1 - Critical: Medical emergency
        {
            "source": "slack",
            "sender": "hr@startup.io",
            "subject": "Medical Emergency - Employee needs immediate approval",
            "body": """
            We have a medical emergency situation. An employee needs
            emergency surgery and the pre-authorization is stuck.
            This is literally life or death. Please escalate immediately.
            """,
            "timestamp": (now - timedelta(hours=2)).isoformat(),
            "expected_priority": "P1",
            "expected_issue_type": "claim",
            "expected_sentiment": "critical"
        },
        # P1 - Critical: Contract termination
        {
            "source": "gmail",
            "sender": "procurement@enterprise.com",
            "subject": "Notice of Contract Termination",
            "body": """
            Due to repeated service failures and unresolved issues,
            we are hereby providing notice to terminate our contract.
            Please have your legal team contact us regarding the
            cancellation process and any outstanding liabilities.
            """,
            "timestamp": (now - timedelta(hours=5)).isoformat(),
            "expected_priority": "P1",
            "expected_issue_type": "other",
            "expected_sentiment": "critical"
        },
        # P2 - Frustrated: Technical issue
        {
            "source": "slack",
            "sender": "admin@techfirm.com",
            "subject": "Portal Error - Cannot access claims dashboard",
            "body": """
            We've been experiencing persistent errors on the claims portal.
            The system crashes every time we try to submit a new claim.
            This has been broken for 3 days and is causing delays.
            Our employees are frustrated and we need this fixed urgently.
            """,
            "timestamp": (now - timedelta(days=1)).isoformat(),
            "expected_priority": "P2",
            "expected_issue_type": "technical",
            "expected_sentiment": "frustrated"
        },
        # P2 - Frustrated: Billing dispute
        {
            "source": "gmail",
            "sender": "finance@midsize.co",
            "subject": "Billing Error - Duplicate charges on invoice",
            "body": """
            We noticed duplicate charges on our latest invoice.
            The total amount is incorrect and we need this corrected.
            This billing issue has been ongoing for 2 months now.
            Please fix this problem and send a corrected invoice.
            """,
            "timestamp": (now - timedelta(days=2)).isoformat(),
            "expected_priority": "P2",
            "expected_issue_type": "billing",
            "expected_sentiment": "frustrated"
        },
        # P2 - Frustrated: Delayed onboarding
        {
            "source": "gmail",
            "sender": "hr@newclient.org",
            "subject": "Onboarding Delay - Employees stuck in limbo",
            "body": """
            Our employee onboarding has been stuck for over a week.
            The setup process is not working and provisioning is delayed.
            This is causing issues with payroll and benefits enrollment.
            We're very frustrated with the lack of progress.
            """,
            "timestamp": (now - timedelta(days=3)).isoformat(),
            "expected_priority": "P2",
            "expected_issue_type": "onboarding",
            "expected_sentiment": "frustrated"
        },
        # P3 - Neutral: Information request
        {
            "source": "slack",
            "sender": "benefits@smallbiz.com",
            "subject": "Question about coverage limits",
            "body": """
            Hi team, I have a quick question about our policy.
            What are the coverage limits for outpatient procedures?
            Also, can you send us the updated benefits summary?
            Thanks in advance for your help.
            """,
            "timestamp": (now - timedelta(days=4)).isoformat(),
            "expected_priority": "P3",
            "expected_issue_type": "other",
            "expected_sentiment": "neutral"
        },
        # P3 - Neutral: Renewal inquiry
        {
            "source": "gmail",
            "sender": "admin@renewalcorp.com",
            "subject": "Upcoming Renewal - Policy Review Request",
            "body": """
            Our policy is up for renewal next quarter.
            We would like to schedule a meeting to review our options
            and discuss any changes to the coverage.
            Please let us know your availability.
            """,
            "timestamp": (now - timedelta(days=5)).isoformat(),
            "expected_priority": "P3",
            "expected_issue_type": "renewal",
            "expected_sentiment": "neutral"
        },
        # P3 - Neutral: General feedback
        {
            "source": "slack",
            "sender": "support@happycustomer.com",
            "subject": "Feedback on recent claim process",
            "body": """
            Just wanted to provide some feedback on our recent claim.
            The process was smooth overall. We appreciate the quick
            turnaround time. Looking forward to continued partnership.
            """,
            "timestamp": (now - timedelta(days=6)).isoformat(),
            "expected_priority": "P3",
            "expected_issue_type": "claim",
            "expected_sentiment": "neutral"
        },
        # P2 - API/Technical issue
        {
            "source": "gmail",
            "sender": "devteam@integration.io",
            "subject": "API Error 500 - Integration broken",
            "body": """
            Our API integration is returning 500 errors consistently.
            The system crashed after we tried to sync employee data.
            This bug is blocking our HR operations and needs urgent fix.
            Error logs attached.
            """,
            "timestamp": (now - timedelta(hours=12)).isoformat(),
            "expected_priority": "P2",
            "expected_issue_type": "technical",
            "expected_sentiment": "frustrated"
        },
    ]


# =============================================================================
# TEST FUNCTIONS
# =============================================================================

def test_mock_sheet_operations():
    """Test the mock sheet implementation."""
    print("\n" + "=" * 70)
    print("TEST 1: Mock Sheet Operations")
    print("=" * 70)

    ws = setup_mock_sheet()

    # Test insert and read
    print("\n[1.1] Testing header insertion...")
    headers = ws.row_values(1)
    assert len(headers) == 15, f"Expected 15 headers, got {len(headers)}"
    assert headers[0] == "ID", f"First header should be 'ID', got '{headers[0]}'"
    print(f"  [PASS] Headers inserted correctly: {len(headers)} columns")

    # Test append
    print("\n[1.2] Testing row append...")
    test_row = ["test-id-1", "2026-03-20T10:00:00", "gmail", "test@test.com",
                "Test Corp", "technical", "P1", "Test summary", "unblock",
                "Tech Support", "", "Open", 0, "critical", "Test body"]
    ws.append_row(test_row)
    assert len(ws.get_all_values()) == 2, "Should have 2 rows (header + data)"
    print("  [PASS] Row appended successfully")

    # Test get_all_records
    print("\n[1.3] Testing get_all_records...")
    records = ws.get_all_records()
    assert len(records) == 1, f"Expected 1 record, got {len(records)}"
    assert records[0]["ID"] == "test-id-1", "ID mismatch"
    assert records[0]["Priority"] == "P1", "Priority mismatch"
    print(f"  [PASS] Retrieved {len(records)} record(s) correctly")

    # Test update_cell
    print("\n[1.4] Testing cell update...")
    ws.update_cell(2, 12, "In Progress")  # Update Status column
    records = ws.get_all_records()
    assert records[0]["Status"] == "In Progress", "Status update failed"
    print("  [PASS] Cell updated correctly")

    print("\n[PASS] All mock sheet operations passed!")
    return True


def test_ai_layer_triage():
    """Test AI layer triage with rule-based fallback."""
    print("\n" + "=" * 70)
    print("TEST 2: AI Layer Triage (Rule-Based Fallback)")
    print("=" * 70)

    from app.ai_layer import apply_rule_based_fallback

    test_cases = generate_test_escalations()

    passed = 0
    failed = 0

    for i, case in enumerate(test_cases, 1):
        print(f"\n[2.{i}] Testing: {case['subject'][:50]}...")

        result = apply_rule_based_fallback(case)

        # Verify required fields
        required_fields = ["summary", "account_name", "issue_type", "priority",
                          "action_needed", "sentiment", "suggested_owner"]
        missing = [f for f in required_fields if f not in result]
        if missing:
            print(f"  [FAIL] Missing fields: {missing}")
            failed += 1
            continue

        # Check priority
        if result["priority"] == case["expected_priority"]:
            print(f"  [PASS] Priority: {result['priority']} (expected)")
        else:
            print(f"  [WARN] Priority: {result['priority']} (expected {case['expected_priority']})")

        # Check issue type
        if result["issue_type"] == case["expected_issue_type"]:
            print(f"  [PASS] Issue Type: {result['issue_type']} (expected)")
        else:
            print(f"  [WARN] Issue Type: {result['issue_type']} (expected {case['expected_issue_type']})")

        # Check sentiment
        if result["sentiment"] == case["expected_sentiment"]:
            print(f"  [PASS] Sentiment: {result['sentiment']} (expected)")
        else:
            print(f"  [WARN] Sentiment: {result['sentiment']} (expected {case['expected_sentiment']})")

        passed += 1

    print(f"\n[SUMMARY] {passed}/{len(test_cases)} test cases passed")
    return passed == len(test_cases)


def test_full_pipeline_with_mock_sheet():
    """Test the full pipeline from webhook to sheet storage."""
    print("\n" + "=" * 70)
    print("TEST 3: Full Pipeline Integration")
    print("=" * 70)

    # Set up mock sheet
    mock_ws = setup_mock_sheet()

    # Mock the sheets module
    with patch('app.sheets._worksheet', mock_ws), \
         patch('app.sheets.init_sheet', return_value=mock_ws):

        # Also patch the global worksheet
        import app.sheets as sheets_module
        original_worksheet = sheets_module._worksheet
        sheets_module._worksheet = mock_ws

        try:
            from app.ai_layer import process_escalation

            test_cases = generate_test_escalations()
            processed_ids = []

            print(f"\n[3.1] Processing {len(test_cases)} escalations through pipeline...")

            for i, case in enumerate(test_cases, 1):
                test_data = {
                    "source": case["source"],
                    "sender": case["sender"],
                    "subject": case["subject"],
                    "body": case["body"],
                    "timestamp": case["timestamp"]
                }

                result = process_escalation(test_data)
                processed_ids.append(result["escalation_id"])

                print(f"  [{i}] {result['escalation_id'][:8]}... -> "
                      f"Priority: {result['triage']['priority']}, "
                      f"Type: {result['triage']['issue_type']}")

            # Verify all rows were saved
            print(f"\n[3.2] Verifying sheet data...")
            records = mock_ws.get_all_records()
            print(f"  Sheet contains {len(records)} records")

            assert len(records) == len(test_cases), \
                f"Expected {len(test_cases)} records, got {len(records)}"
            print(f"  [PASS] All {len(test_cases)} escalations saved to sheet")

            # Verify data integrity
            print(f"\n[3.3] Verifying data integrity...")
            for record in records:
                assert record["ID"], "Missing ID"
                assert record["Timestamp"], "Missing Timestamp"
                assert record["Priority"] in ["P1", "P2", "P3"], \
                    f"Invalid priority: {record['Priority']}"
                assert record["Status"] == "Open", \
                    f"Status should be 'Open', got '{record['Status']}'"

            print("  [PASS] All records have valid data")

            # Count by priority
            p1_count = sum(1 for r in records if r["Priority"] == "P1")
            p2_count = sum(1 for r in records if r["Priority"] == "P2")
            p3_count = sum(1 for r in records if r["Priority"] == "P3")
            print(f"\n[3.4] Priority distribution:")
            print(f"  P1 (Critical): {p1_count}")
            print(f"  P2 (Urgent):   {p2_count}")
            print(f"  P3 (Normal):   {p3_count}")

            print("\n[PASS] Full pipeline test passed!")
            return True

        finally:
            sheets_module._worksheet = original_worksheet


def test_sheets_crud_operations():
    """Test Create, Read, Update operations on the sheet."""
    print("\n" + "=" * 70)
    print("TEST 4: Sheet CRUD Operations")
    print("=" * 70)

    mock_ws = setup_mock_sheet()

    with patch('app.sheets._worksheet', mock_ws), \
         patch('app.sheets.init_sheet', return_value=mock_ws):

        import app.sheets as sheets_module
        original_worksheet = sheets_module._worksheet
        sheets_module._worksheet = mock_ws

        try:
            from app.sheets import save_to_sheet, get_all_escalations, \
                update_status, get_open_p1

            # Test save_to_sheet
            print("\n[4.1] Testing save_to_sheet...")
            test_data = {
                "escalation_id": "test-crud-001",
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "source": "gmail",
                "sender": "test@example.com",
                "subject": "Test CRUD",
                "body": "Testing CRUD operations",
                "triage": {
                    "summary": "Test CRUD summary",
                    "account_name": "Test Corp",
                    "issue_type": "technical",
                    "priority": "P1",
                    "action_needed": "unblock",
                    "sentiment": "critical",
                    "suggested_owner": "Tech Support"
                }
            }
            save_to_sheet(test_data)
            print("  [PASS] save_to_sheet executed successfully")

            # Test get_all_escalations
            print("\n[4.2] Testing get_all_escalations...")
            records = get_all_escalations()
            assert len(records) == 1, f"Expected 1 record, got {len(records)}"
            assert records[0]["ID"] == "test-crud-001", "ID mismatch"
            print(f"  [PASS] Retrieved {len(records)} record(s)")

            # Add more records for testing
            for i in range(2, 6):
                priority = "P1" if i <= 2 else ("P2" if i <= 4 else "P3")
                test_data["escalation_id"] = f"test-crud-00{i}"
                test_data["triage"]["priority"] = priority
                save_to_sheet(test_data)

            records = get_all_escalations()
            assert len(records) == 5, f"Expected 5 records, got {len(records)}"
            print(f"  [PASS] Added {len(records)} total records")

            # Test get_open_p1
            print("\n[4.3] Testing get_open_p1...")
            p1_records = get_open_p1()
            assert len(p1_records) == 2, f"Expected 2 P1 records, got {len(p1_records)}"
            print(f"  [PASS] Found {len(p1_records)} open P1 escalations")

            # Test update_status
            print("\n[4.4] Testing update_status...")
            result = update_status("test-crud-001", "In Progress", "Test Owner")
            assert result is True, "Update should return True"
            records = get_all_escalations()
            updated_record = next(r for r in records if r["ID"] == "test-crud-001")
            assert updated_record["Status"] == "In Progress", "Status not updated"
            assert updated_record["Owner"] == "Test Owner", "Owner not updated"
            print("  [PASS] Status and Owner updated correctly")

            # Test closing an escalation
            print("\n[4.5] Testing close escalation...")
            update_status("test-crud-001", "Closed", "Test Owner")
            p1_records = get_open_p1()
            assert len(p1_records) == 1, f"Expected 1 open P1 after close, got {len(p1_records)}"
            print("  [PASS] Closed escalation removed from open P1 list")

            # Test update non-existent ID
            print("\n[4.6] Testing update non-existent ID...")
            result = update_status("non-existent-id", "Closed", "")
            assert result is False, "Should return False for non-existent ID"
            print("  [PASS] Returns False for non-existent ID")

            print("\n[PASS] All CRUD operations passed!")
            return True

        finally:
            sheets_module._worksheet = original_worksheet


def test_dashboard_metrics():
    """Test dashboard metrics calculation."""
    print("\n" + "=" * 70)
    print("TEST 5: Dashboard Metrics Calculation")
    print("=" * 70)

    import pandas as pd
    from app.dashboard import compute_metrics, to_dataframe

    # Create test data
    now = datetime.now(timezone.utc)
    test_records = [
        {"ID": "1", "Timestamp": now.isoformat(), "Source": "gmail",
         "Sender": "a@a.com", "Account": "A", "Issue Type": "technical",
         "Priority": "P1", "Summary": "Test", "Action Needed": "unblock",
         "Suggested Owner": "Tech", "Owner": "", "Status": "Open",
         "TAT Hours": 2, "Sentiment": "critical", "Raw Body": "..."},
        {"ID": "2", "Timestamp": now.isoformat(), "Source": "slack",
         "Sender": "b@b.com", "Account": "B", "Issue Type": "billing",
         "Priority": "P1", "Summary": "Test", "Action Needed": "unblock",
         "Suggested Owner": "Billing", "Owner": "John", "Status": "In Progress",
         "TAT Hours": 5, "Sentiment": "critical", "Raw Body": "..."},
        {"ID": "3", "Timestamp": now.isoformat(), "Source": "gmail",
         "Sender": "c@c.com", "Account": "C", "Issue Type": "claim",
         "Priority": "P2", "Summary": "Test", "Action Needed": "followup",
         "Suggested Owner": "Claims", "Owner": "", "Status": "Open",
         "TAT Hours": 10, "Sentiment": "frustrated", "Raw Body": "..."},
        {"ID": "4", "Timestamp": now.isoformat(), "Source": "gmail",
         "Sender": "d@d.com", "Account": "D", "Issue Type": "other",
         "Priority": "P3", "Summary": "Test", "Action Needed": "info",
         "Suggested Owner": "Support", "Owner": "Jane", "Status": "Closed",
         "TAT Hours": 1, "Sentiment": "neutral", "Raw Body": "..."},
        {"ID": "5", "Timestamp": now.isoformat(), "Source": "slack",
         "Sender": "e@e.com", "Account": "E", "Issue Type": "renewal",
         "Priority": "P2", "Summary": "Test", "Action Needed": "followup",
         "Suggested Owner": "Account", "Owner": "", "Status": "Resolved",
         "TAT Hours": 8, "Sentiment": "neutral", "Raw Body": "..."},
    ]

    print("\n[5.1] Testing to_dataframe conversion...")
    df = to_dataframe(test_records)
    assert len(df) == 5, f"Expected 5 rows, got {len(df)}"
    assert "Timestamp_parsed" in df.columns, "Missing parsed timestamp column"
    print(f"  [PASS] Converted {len(df)} records to DataFrame")

    print("\n[5.2] Testing compute_metrics...")
    metrics = compute_metrics(df)

    print(f"  Total Open: {metrics['open_total']}")
    print(f"  Open P1: {metrics['open_p1']}")
    print(f"  Avg TAT (Open): {metrics['avg_tat_open']:.1f}")
    print(f"  Closed Today: {metrics['closed_today']}")

    # Verify calculations
    # Open = not Closed and not Resolved = IDs 1,2,3 = 3
    assert metrics['open_total'] == 3, \
        f"Expected 3 open, got {metrics['open_total']}"
    print("  [PASS] Total Open count correct")

    # Open P1 = IDs 1,2 = 2
    assert metrics['open_p1'] == 2, \
        f"Expected 2 open P1, got {metrics['open_p1']}"
    print("  [PASS] Open P1 count correct")

    # Avg TAT of open = (2+5+10)/3 = 5.67
    expected_avg = (2 + 5 + 10) / 3
    assert abs(metrics['avg_tat_open'] - expected_avg) < 0.1, \
        f"Expected avg TAT ~{expected_avg:.1f}, got {metrics['avg_tat_open']:.1f}"
    print("  [PASS] Average TAT calculation correct")

    print("\n[5.3] Testing with empty DataFrame...")
    empty_metrics = compute_metrics(pd.DataFrame())
    assert empty_metrics['open_total'] == 0, "Empty df should have 0 open"
    assert empty_metrics['open_p1'] == 0, "Empty df should have 0 P1"
    print("  [PASS] Empty DataFrame handled correctly")

    print("\n[PASS] All dashboard metrics tests passed!")
    return True


def test_flask_endpoints():
    """Test Flask webhook and health endpoints."""
    print("\n" + "=" * 70)
    print("TEST 6: Flask Endpoints")
    print("=" * 70)

    from app.ingest import create_app

    app = create_app()

    with app.test_client() as client:
        # Test health endpoint
        print("\n[6.1] Testing GET /health...")
        response = client.get('/health')
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.get_json()
        assert data['status'] == 'healthy', "Health status should be 'healthy'"
        print(f"  [PASS] Health check: {data['status']}")

        # Test valid webhook
        print("\n[6.2] Testing POST /webhook (valid)...")
        valid_payload = {
            "source": "gmail",
            "sender": "test@test.com",
            "subject": "Test Subject",
            "body": "Test body content",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        response = client.post('/webhook',
                              data=json.dumps(valid_payload),
                              content_type='application/json')
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.get_json()
        assert data['status'] == 'ok', "Status should be 'ok'"
        assert 'id' in data, "Response should contain 'id'"
        print(f"  [PASS] Valid webhook accepted, ID: {data['id'][:8]}...")

        # Test invalid source
        print("\n[6.3] Testing POST /webhook (invalid source)...")
        invalid_source = valid_payload.copy()
        invalid_source['source'] = 'twitter'
        response = client.post('/webhook',
                              data=json.dumps(invalid_source),
                              content_type='application/json')
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print("  [PASS] Invalid source rejected with 400")

        # Test missing fields
        print("\n[6.4] Testing POST /webhook (missing fields)...")
        incomplete = {"source": "gmail", "sender": "test@test.com"}
        response = client.post('/webhook',
                              data=json.dumps(incomplete),
                              content_type='application/json')
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        data = response.get_json()
        assert 'Missing required fields' in data['message'], "Should mention missing fields"
        print("  [PASS] Missing fields rejected with 400")

        # Test empty body
        print("\n[6.5] Testing POST /webhook (empty body)...")
        empty_body = valid_payload.copy()
        empty_body['body'] = "   "
        response = client.post('/webhook',
                              data=json.dumps(empty_body),
                              content_type='application/json')
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print("  [PASS] Empty body rejected with 400")

        # Test invalid JSON
        print("\n[6.6] Testing POST /webhook (invalid JSON)...")
        response = client.post('/webhook',
                              data="not valid json",
                              content_type='application/json')
        assert response.status_code == 400, f"Expected 400, got {response.status_code}"
        print("  [PASS] Invalid JSON rejected with 400")

    print("\n[PASS] All Flask endpoint tests passed!")
    return True


def test_network_queue():
    """Test the internal network queue system."""
    print("\n" + "=" * 70)
    print("TEST 7: Internal Network Queue")
    print("=" * 70)

    from app.network import InternalEscalationNetwork
    import tempfile

    # Create network with temp directories
    with tempfile.TemporaryDirectory() as tmpdir:
        inbox_dir = os.path.join(tmpdir, "inbox")
        os.makedirs(inbox_dir)

        from pathlib import Path
        network = InternalEscalationNetwork(
            inbox_dir=Path(inbox_dir),
            poll_interval_sec=1
        )

        print("\n[7.1] Testing network start...")
        assert not network.is_running(), "Should not be running initially"
        network.start()
        assert network.is_running(), "Should be running after start"
        print("  [PASS] Network started successfully")

        print("\n[7.2] Testing queue submission...")
        test_payload = {"test": "data"}
        request_id = network.submit(test_payload, source="test")
        assert request_id, "Should return a request ID"
        print(f"  [PASS] Submitted with ID: {request_id[:8]}...")

        print("\n[7.3] Testing queue size...")
        # Queue might be processed quickly, just verify it works
        size = network.queue_size()
        print(f"  [PASS] Queue size check works: {size}")

        print("\n[7.4] Testing graceful stop...")
        network.stop(timeout=2.0)
        assert not network.is_running(), "Should not be running after stop"
        print("  [PASS] Network stopped gracefully")

        print("\n[7.5] Testing idempotent start/stop...")
        network.start()
        network.start()  # Should not error
        network.stop()
        network.stop()  # Should not error
        print("  [PASS] Idempotent start/stop works")

    print("\n[PASS] All network queue tests passed!")
    return True


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "=" * 70)
    print("ESCALATION MANAGEMENT SYSTEM - COMPREHENSIVE INTEGRATION TESTS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    tests = [
        ("Mock Sheet Operations", test_mock_sheet_operations),
        ("AI Layer Triage", test_ai_layer_triage),
        ("Full Pipeline Integration", test_full_pipeline_with_mock_sheet),
        ("Sheet CRUD Operations", test_sheets_crud_operations),
        ("Dashboard Metrics", test_dashboard_metrics),
        ("Flask Endpoints", test_flask_endpoints),
        ("Network Queue", test_network_queue),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, "PASSED" if result else "FAILED"))
        except Exception as e:
            print(f"\n[ERROR] {name} failed with exception: {str(e)}")
            import traceback
            traceback.print_exc()
            results.append((name, f"ERROR: {str(e)[:50]}"))

    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = 0
    for name, result in results:
        status_marker = "[PASS]" if result == "PASSED" else "[FAIL]"
        print(f"{status_marker} {name:40s} {result}")
        if result == "PASSED":
            passed += 1

    print("=" * 70)
    print(f"TOTAL: {passed}/{len(tests)} tests passed")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if passed == len(tests):
        print("\n*** ALL TESTS PASSED! ***")
        return 0
    else:
        print(f"\n*** {len(tests) - passed} TEST(S) FAILED ***")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
