#!/usr/bin/env python3
"""
Test script to verify the webhook and AI layer integration.
"""

import json
import sys
from datetime import datetime, timezone

# Test data
test_webhook_data = {
    "source": "gmail",
    "sender": "john.doe@acme.com",
    "subject": "URGENT: Claim denial escalation - legal threat",
    "body": """
    We have a critical situation. A customer is threatening legal action because
    their claim was denied incorrectly. This is a potential IRDAI compliance issue
    and we need immediate attention from our legal team. The customer is very frustrated
    and has mentioned canceling their entire contract with us.
    """,
    "timestamp": datetime.now(timezone.utc).isoformat()
}

def test_process_escalation():
    """Test the process_escalation function."""
    print("=" * 70)
    print("Testing process_escalation function")
    print("=" * 70)
    
    try:
        from app.ai_layer import process_escalation
        
        print("\nInput webhook data:")
        print(json.dumps(test_webhook_data, indent=2))
        
        print("\nProcessing escalation...")
        result = process_escalation(test_webhook_data)
        
        print("\nResult:")
        print(json.dumps(result, indent=2))
        
        # Validate output
        assert "escalation_id" in result, "Missing escalation_id"
        assert "processed_at" in result, "Missing processed_at"
        assert "triage" in result, "Missing triage data"
        assert "priority" in result["triage"], "Missing priority in triage"
        assert result["triage"]["priority"] in ["P1", "P2", "P3"], f"Invalid priority: {result['triage']['priority']}"
        
        print("\n[PASS] All assertions passed!")
        print(f"[PASS] Escalation ID: {result['escalation_id']}")
        print(f"[PASS] Priority: {result['triage']['priority']}")
        print(f"[PASS] Issue Type: {result['triage']['issue_type']}")
        print(f"[PASS] Sentiment: {result['triage']['sentiment']}")
        
        return True
    except Exception as e:
        print(f"\n[FAIL] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_flask_app():
    """Test the Flask app creation."""
    print("\n" + "=" * 70)
    print("Testing Flask app creation")
    print("=" * 70)
    
    try:
        from app.ingest import create_app
        
        app = create_app()
        print("[PASS] Flask app created successfully")
        
        # Test with test client
        with app.test_client() as client:
            # Test POST /webhook with valid data
            print("\nTesting POST /webhook with valid data...")
            response = client.post(
                '/webhook',
                data=json.dumps(test_webhook_data),
                content_type='application/json'
            )
            
            print(f"Status code: {response.status_code}")
            result = response.get_json()
            print(f"Response: {json.dumps(result, indent=2)}")
            
            assert response.status_code == 200, f"Expected 200, got {response.status_code}"
            assert result["status"] == "ok", f"Expected status='ok', got {result.get('status')}"
            assert "id" in result, "Missing id in response"
            
            print("[PASS] Valid webhook test passed!")
            
            # Test POST /webhook with missing fields
            print("\nTesting POST /webhook with missing fields...")
            invalid_data = {"source": "gmail", "sender": "test@example.com"}
            response = client.post(
                '/webhook',
                data=json.dumps(invalid_data),
                content_type='application/json'
            )
            
            print(f"Status code: {response.status_code}")
            print(f"Response: {json.dumps(response.get_json(), indent=2)}")
            
            assert response.status_code == 400, f"Expected 400, got {response.status_code}"
            assert response.get_json()["status"] == "error", "Expected error status"
            
            print("[PASS] Invalid webhook test passed!")
            
        return True
    except Exception as e:
        print(f"\n[FAIL] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("ESCALATION MANAGEMENT SYSTEM - INTEGRATION TEST")
    print("=" * 70)
    
    test1 = test_process_escalation()
    test2 = test_flask_app()
    
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"process_escalation test: {'PASSED' if test1 else 'FAILED'}")
    print(f"Flask app test: {'PASSED' if test2 else 'FAILED'}")

    if test1 and test2:
        print("\n*** All tests passed! ***")
        sys.exit(0)
    else:
        print("\n*** Some tests failed ***")
        sys.exit(1)
