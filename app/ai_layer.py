"""
AI layer module for summarizing and prioritizing escalation events.

This module interfaces with Ollama to analyze escalation messages,
generate summaries, and determine priority levels using advanced LLM capabilities.
"""

import requests
import logging
import json
import uuid
import os
from typing import Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# System prompt for Plum Insurance escalation triage
SYSTEM_PROMPT = """You are an escalation triage assistant for a senior VP at a health insurance platform called Plum. 
Analyze the message and respond ONLY with valid JSON, no other text. 
JSON schema:
{
  "summary": "string (max 2 sentences, what happened and what is needed)",
  "account_name": "string (company name if mentioned, else 'Unknown')",
  "issue_type": "one of [claim, onboarding, technical, renewal, billing, other]",
  "priority": "one of [P1, P2, P3] where P1=churn risk/medical emergency/legal threat, P2=delayed SLA/frustrated customer, P3=info request/low urgency",
  "action_needed": "one of [decision, followup, unblock, info]",
  "sentiment": "one of [critical, frustrated, neutral]",
  "suggested_owner": "string (guess based on issue type: Claims Team, Onboarding Team, Tech Support, Account Manager)"
}"""

# Keywords for rule-based fallback
P1_KEYWORDS = {"legal", "irdai", "cancel", "terminate", "lawsuit", "compliance", "emergency", "medical", "death"}
P2_KEYWORDS = {"urgent", "stuck", "delay", "not working", "broken", "error", "issue", "problem", "frustrated"}


def call_ollama(prompt: str, ollama_url: str, model: str = "llama3") -> Optional[str]:
    """
    Call Ollama API with the given prompt.
    
    Args:
        prompt: The prompt to send to Ollama
        ollama_url: URL of Ollama service
        model: Model name (tries llama3 first, falls back to mistral)
        
    Returns:
        str: Response from Ollama, or None if call fails
    """
    try:
        url = f"{ollama_url}/api/generate"
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False
        }
        
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        return result.get("response", "")
    except requests.exceptions.RequestException as e:
        logger.error(f"Ollama API error with model {model}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error calling Ollama: {str(e)}")
        return None


def parse_triage_response(response_text: str) -> Optional[Dict[str, Any]]:
    """
    Parse JSON triage response from Ollama.
    
    Args:
        response_text: Raw response text from Ollama
        
    Returns:
        dict: Parsed triage data, or None if parsing fails
    """
    if not response_text:
        return None
    
    try:
        # Try to extract JSON from the response (in case there's extra text)
        response_text = response_text.strip()
        
        # Handle case where model returns markdown code blocks
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        response_text = response_text.strip()
        
        # Parse JSON
        data = json.loads(response_text)
        
        # Validate required fields
        required_fields = {"summary", "account_name", "issue_type", "priority", "action_needed", "sentiment", "suggested_owner"}
        if not all(field in data for field in required_fields):
            logger.warning(f"Parsed JSON missing fields: {required_fields - set(data.keys())}")
            return None
        
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Ollama JSON response: {str(e)}")
        logger.debug(f"Raw response: {response_text[:200]}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing triage response: {str(e)}")
        return None


def apply_rule_based_fallback(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply keyword-based rules to determine priority when AI is unavailable.
    
    Args:
        data: The escalation data dict
        
    Returns:
        dict: Triage data with priority determined by rules
    """
    combined_text = (
        data.get("subject", "") + " " + 
        data.get("body", "") + " " + 
        data.get("sender", "")
    ).lower()
    
    # Check for P1 keywords
    if any(keyword in combined_text for keyword in P1_KEYWORDS):
        priority = "P1"
        sentiment = "critical"
    # Check for P2 keywords
    elif any(keyword in combined_text for keyword in P2_KEYWORDS):
        priority = "P2"
        sentiment = "frustrated"
    # Default to P3
    else:
        priority = "P3"
        sentiment = "neutral"
    
    # Determine issue type from keywords
    if any(kw in combined_text for kw in {"claim", "denied", "rejected", "reimburs"}):
        issue_type = "claim"
        suggested_owner = "Claims Team"
    elif any(kw in combined_text for kw in {"onboard", "setup", "enroll", "provision"}):
        issue_type = "onboarding"
        suggested_owner = "Onboarding Team"
    elif any(kw in combined_text for kw in {"bug", "error", "crash", "technical", "system", "api"}):
        issue_type = "technical"
        suggested_owner = "Tech Support"
    elif any(kw in combined_text for kw in {"renew", "renewal", "expire", "expiration"}):
        issue_type = "renewal"
        suggested_owner = "Account Manager"
    elif any(kw in combined_text for kw in {"bill", "invoice", "cost", "payment", "charge", "fee"}):
        issue_type = "billing"
        suggested_owner = "Billing Team"
    else:
        issue_type = "other"
        suggested_owner = "Account Manager"
    
    return {
        "summary": data.get("subject", "No subject provided"),
        "account_name": data.get("sender", "Unknown"),
        "issue_type": issue_type,
        "priority": priority,
        "action_needed": "followup" if priority == "P1" else "info",
        "sentiment": sentiment,
        "suggested_owner": suggested_owner
    }


def process_escalation(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process an escalation event through the AI layer using Ollama.
    
    Combines the input data with AI-generated triage information. If Ollama is unavailable,
    uses rule-based keyword matching for priority determination.
    
    Args:
        data: The escalation event with keys:
            - source: "gmail" or "slack"
            - sender: Email or Slack user
            - subject: Subject line
            - body: Full message body
            - timestamp: ISO8601 timestamp
            
    Returns:
        dict: Enriched event with AI triage data and escalation ID
    """
    escalation_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()
    
    logger.info(f"Processing escalation {escalation_id} from {data.get('sender', 'unknown')}")
    
    # Build the full prompt
    full_prompt = f"{SYSTEM_PROMPT}\n\nMessage to analyze:\n{data.get('body', '')}"
    
    # Try to get AI triage from Ollama
    ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
    triage_data = None
    
    try:
        logger.info(f"[{escalation_id}] Calling Ollama at {ollama_url}")
        response = call_ollama(full_prompt, ollama_url, model="llama3")
        
        if response:
            triage_data = parse_triage_response(response)
            if triage_data:
                logger.info(f"[{escalation_id}] AI triage successful: priority={triage_data.get('priority')}")
        
        # Fallback to mistral if llama3 fails
        if not triage_data:
            logger.info(f"[{escalation_id}] Retrying with mistral model")
            response = call_ollama(full_prompt, ollama_url, model="mistral")
            if response:
                triage_data = parse_triage_response(response)
                if triage_data:
                    logger.info(f"[{escalation_id}] AI triage successful with mistral: priority={triage_data.get('priority')}")
    
    except Exception as e:
        logger.error(f"[{escalation_id}] Error calling Ollama: {str(e)}")
    
    # If AI failed, use rule-based fallback
    if not triage_data:
        logger.warning(f"[{escalation_id}] Using rule-based fallback for triage")
        triage_data = apply_rule_based_fallback(data)
    
    # Merge original data with AI triage
    enriched_data = {
        "escalation_id": escalation_id,
        "processed_at": timestamp,
        **data,  # Original webhook data
        "triage": triage_data  # AI/rule-based triage
    }
    
    # Attempt to save to Google Sheets
    try:
        _save_to_sheet(enriched_data)
    except Exception as e:
        logger.error(f"[{escalation_id}] Error saving to sheet: {str(e)}")
        # Don't fail the whole process if sheet save fails

    logger.info(f"[{escalation_id}] Escalation processing complete")
    return enriched_data


def _save_to_sheet(enriched_data: Dict[str, Any]) -> None:
    """
    Save enriched escalation data to Google Sheets.
    
    Args:
        enriched_data: The enriched escalation dict with triage information
        
    Raises:
        Exception: If sheet operation fails
    """
    try:
        from .sheets import save_to_sheet as sheets_save
        
        sheets_save(enriched_data)
        logger.info(f"Escalation {enriched_data.get('escalation_id')} saved to sheet")
    except ImportError:
        logger.warning("sheets module not available for saving")
    except Exception as e:
        logger.error(f"Failed to save to sheet: {str(e)}")
        # Don't raise - sheet save is optional and shouldn't block webhook processing
