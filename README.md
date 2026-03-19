# Escalation Management System

A VP-level escalation monitoring and triage system with AI-powered classification, Google Sheets integration, and a real-time Streamlit dashboard.

## Quick Start (Demo Mode)

```bash
# Clone and enter directory
cd escalation-system

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run dashboard
streamlit run app/dashboard.py
```

Dashboard opens at `http://localhost:8501` with 12 sample escalations. No configuration required.

## Features

- **Webhook API**: Flask server accepting escalation events
- **AI Triage**: Ollama LLM classification with rule-based fallback
- **Demo Mode**: Works without external dependencies
- **Google Sheets Integration**: Optional persistent storage with retry logic
- **Streamlit Dashboard**: Real-time monitoring with P1 alerts and filtering
- **Internal Queue**: Background processing with inbox file polling

## Project Structure

```
escalation-system/
├── app/
│   ├── __init__.py
│   ├── ingest.py        # Flask webhook server
│   ├── ai_layer.py      # AI triage and classification
│   ├── sheets.py        # Google Sheets / demo data storage
│   ├── network.py       # Internal queue and background workers
│   └── dashboard.py     # Streamlit VP dashboard
├── runtime/
│   ├── inbox/           # JSON files for batch processing
│   ├── processed/       # Successfully processed files
│   └── failed/          # Failed processing files
├── test_integration.py  # Integration test suite (7 tests)
├── test_sheets.py       # Sheets module tests
├── test_webhook.py      # Webhook endpoint tests
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.9+
- pip

### Optional
- **Ollama** with `llama3.2` model (falls back to rule-based triage if unavailable)
- **Google Cloud** service account credentials (falls back to demo mode if unavailable)

## Installation

1. **Create virtual environment:**
```bash
python -m venv venv
```

2. **Activate:**
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Running the System

### Dashboard Only (Demo Mode)

```bash
streamlit run app/dashboard.py
```

Opens at `http://localhost:8501` with 12 sample escalations.

### Full System (Webhook + Dashboard)

**Terminal 1 - Webhook Server:**
```bash
python -m app.ingest
```
Runs on `http://localhost:5000`

**Terminal 2 - Dashboard:**
```bash
streamlit run app/dashboard.py
```
Runs on `http://localhost:8501`

## Configuration (Optional)

Create `.env` file in project root for production mode:

```env
# Google Sheets (enables persistent storage)
GOOGLE_CREDENTIALS_PATH=./credentials.json
GOOGLE_SHEET_ID=your_sheet_id_here
SHEET_NAME=Escalations

# Ollama (enables AI triage)
OLLAMA_URL=http://localhost:11434

# Flask server
FLASK_PORT=5000
```

Without these variables, the system runs in demo mode automatically.

## API Reference

### POST /webhook

Submit an escalation event.

**Request:**
```json
{
  "source": "gmail",
  "sender": "user@example.com",
  "subject": "Urgent: Claim denied",
  "body": "Our claim was denied and we need immediate help...",
  "timestamp": "2026-03-20T10:00:00Z"
}
```

**Response:**
```json
{
  "status": "received",
  "request_id": "uuid-string"
}
```

**Example:**
```bash
curl -X POST http://localhost:5000/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "source": "gmail",
    "sender": "cfo@company.com",
    "subject": "Critical: Legal action",
    "body": "Preparing legal action regarding denied claim.",
    "timestamp": "2026-03-20T10:00:00Z"
  }'
```

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy"
}
```

## Testing

Run all tests:
```bash
python test_integration.py
python test_sheets.py
python test_webhook.py
```

All tests should pass with `[OK]` status.

## Demo Mode vs Production Mode

| Feature | Demo Mode | Production Mode |
|---------|-----------|-----------------|
| Data Storage | In-memory | Google Sheets |
| Sample Data | 12 pre-loaded escalations | Live data |
| Configuration | None required | Credentials + Sheet ID |
| Persistence | Session only | Permanent |
| AI Triage | Rule-based | Ollama LLM |

The system automatically detects missing credentials and switches to demo mode.

## Dashboard Features

- **Metrics Row**: Total Open, P1 Critical, Avg TAT Hours, Closed Today
- **P1 Alerts Section**: Expandable cards with ownership assignment
- **Full Table**: Filterable by Priority, Status, Issue Type, Source
- **Manual Escalation**: Submit new escalations via sidebar form
- **Auto-refresh**: Updates every 60 seconds

## Priority Levels

| Priority | Color | Description |
|----------|-------|-------------|
| P1 | Red | Critical - Immediate action required |
| P2 | Orange | High - Urgent attention needed |
| P3 | Green | Normal - Standard handling |

## File-Based Processing

Drop JSON files into `runtime/inbox/` for automatic processing:

```json
{
  "source": "gmail",
  "sender": "user@company.com",
  "subject": "Urgent claim issue",
  "body": "Customer escalation message",
  "timestamp": "2026-03-20T10:00:00Z"
}
```

The network poller automatically:
1. Reads inbox files
2. Queues them for processing
3. Moves success files to `runtime/processed/`
4. Moves failed files to `runtime/failed/`

## Google Sheets Setup (Optional)

1. Create a Google Cloud project
2. Enable Google Sheets API
3. Create a service account
4. Download JSON credentials
5. Share your Google Sheet with the service account email
6. Set environment variables in `.env`

## Troubleshooting

**Dashboard won't start:**
```bash
# Kill existing Streamlit processes
taskkill /F /IM streamlit.exe  # Windows
pkill -f streamlit             # Linux/Mac

# Restart
streamlit run app/dashboard.py
```

**Port already in use:**
```bash
streamlit run app/dashboard.py --server.port 8502
FLASK_PORT=5001 python -m app.ingest
```

**Module not found:**
```bash
# Ensure virtual environment is activated
# Ensure you're in the project root directory
pip install -r requirements.txt
```

## Dependencies

```
flask
gspread
google-auth
google-auth-httplib2
google-auth-oauthlib
requests
streamlit
streamlit-autorefresh
python-dotenv
pandas
```

## License

MIT
