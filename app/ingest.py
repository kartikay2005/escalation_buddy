"""
Webhook ingest module for receiving escalation events.

This module provides Flask endpoints to receive webhooks from external systems,
validate incoming data, and forward to the AI layer for processing.
"""

from flask import Flask, request, jsonify
from typing import Dict, Any, Tuple
import logging
import json
import uuid
import os
from datetime import datetime
from dotenv import load_dotenv

try:
    from .network import get_network
except ImportError:
    from network import get_network

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Required fields for webhook payload
REQUIRED_FIELDS = {"source", "sender", "subject", "body", "timestamp"}
VALID_SOURCES = {"gmail", "slack"}


def create_app() -> Flask:
    """
    Create and configure the Flask application.
    
    Returns:
        Flask: Configured Flask application instance
    """
    app = Flask(__name__)
    get_network().start()
    
    @app.route('/webhook', methods=['POST'])
    def webhook() -> Tuple[Dict[str, Any], int]:
        """
        Receive escalation webhook with validation and processing.
        
        Expected JSON payload schema:
        {
            "source": "gmail|slack",
            "sender": "string",
            "subject": "string",
            "body": "string",
            "timestamp": "ISO8601 string (e.g., 2026-03-20T10:30:00Z)"
        }
        
        Returns:
            tuple: (response_dict, status_code)
                - Success: ({"status": "ok", "id": "<uuid>"}, 200)
                - Error: ({"status": "error", "message": "..."}, 400 or 500)
        """
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        request_id = str(uuid.uuid4())[:8]
        
        try:
            # Parse JSON payload (silent=True returns None on parse error instead of raising)
            data = request.get_json(silent=True, force=True)
            if data is None:
                logger.warning(f"[{request_id}] {timestamp} - Invalid JSON payload")
                return {
                    "status": "error",
                    "message": "Invalid JSON payload"
                }, 400
            
            logger.info(f"[{request_id}] {timestamp} - Received webhook from {data.get('source', 'unknown')} "
                       f"sender: {data.get('sender', 'unknown')}")
            
            # Validate required fields
            missing_fields = REQUIRED_FIELDS - set(data.keys())
            if missing_fields:
                logger.warning(f"[{request_id}] {timestamp} - Missing required fields: {missing_fields}")
                return {
                    "status": "error",
                    "message": f"Missing required fields: {', '.join(sorted(missing_fields))}"
                }, 400
            
            # Validate source field
            if data["source"] not in VALID_SOURCES:
                logger.warning(f"[{request_id}] {timestamp} - Invalid source: {data['source']}")
                return {
                    "status": "error",
                    "message": f"Invalid source. Must be one of: {', '.join(VALID_SOURCES)}"
                }, 400
            
            # Validate that fields are non-empty strings
            for field in ["sender", "subject", "body", "timestamp"]:
                if not isinstance(data[field], str) or not data[field].strip():
                    logger.warning(f"[{request_id}] {timestamp} - Empty or invalid {field} field")
                    return {
                        "status": "error",
                        "message": f"Field '{field}' must be a non-empty string"
                    }, 400
            
            # Generate unique ID for this escalation
            escalation_id = str(uuid.uuid4())
            
            # Log successful validation
            logger.info(f"[{request_id}] {timestamp} - Validation passed for escalation {escalation_id}")
            
            # Submit to internal always-on network queue (async processing)
            try:
                network = get_network()
                queued_request_id = network.submit(data, source="webhook")
                logger.info(
                    f"[{request_id}] {timestamp} - Escalation {escalation_id} queued as {queued_request_id}"
                )
            except Exception as e:
                logger.error(f"[{request_id}] {timestamp} - Error queueing escalation: {str(e)}", exc_info=True)
                return {
                    "status": "error",
                    "message": "Unable to queue escalation"
                }, 500
            
            logger.info(f"[{request_id}] {timestamp} - Webhook completed successfully. ID: {escalation_id}")
            return {
                "status": "ok",
                "id": escalation_id
            }, 200
            
        except Exception as e:
            logger.error(f"[{request_id}] {timestamp} - Unexpected error: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": "Internal server error"
            }, 500

    @app.route('/health', methods=['GET'])
    def health() -> Tuple[Dict[str, Any], int]:
        """
        Health check endpoint for monitoring.

        Returns:
            tuple: ({"status": "healthy", "timestamp": "..."}, 200)
        """
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        }, 200

    return app


if __name__ == "__main__":
    port = int(os.getenv("FLASK_PORT", 5000))
    logger.info(f"Starting Escalation Webhook Server on port {port}...")
    app = create_app()
    app.run(host="0.0.0.0", port=port, debug=False)
