# HCMUS Email Ingestion Module (Client Side)

Pulls emails from Gmail / Outlook, keeps only the ones that look like HCMUS
"điểm rèn luyện" (ĐRL) events, hashes them, and ships them to the shared
extraction server. It does **not** do LLM extraction, scraping, or scheduling
(those live on the server, in `vtt/`).

## Pipeline

```
authenticate -> build_query -> fetch_emails -> privacy/keyword gate -> hash -> POST to server
```

| Step | Where |
|---|---|
| Provider interface (shared by Gmail & Outlook) | `mail_provider.py` |
| Gmail implementation | `gmail/gmail.py` |
| Outlook implementation (Microsoft Graph) | `outlook/outlook.py` |
| Allowed senders, keywords, 3-stage gate, hash | `privacy_filter.py` |
| Settings | `filter_config.json` |
| Command-line runner + HTTP shipper | `run_ingestion.py` |

## Setup

1. Python 3.14 with the project's virtualenv: `.venv/bin/python3` (macOS) — the `.venv/bin/pip` script has a broken path, so use `.venv/bin/python3 -m pip ...`.
2. Packages already installed: `google-api-python-client`, `google-auth-oauthlib`, `msal`, `requests`.
3. **Gmail**: put your OAuth client file at `gmail/credentials.json`. On first run a browser opens for consent; the token is cached in `gmail/token.json`.
4. **Outlook**: no file needed. See "Outlook sign-in" below.

## Configure — `filter_config.json`

| Key | Meaning |
|---|---|
| `allowed_senders` | Domains (`hcmus.edu.vn`, also matches subdomains) or full addresses. Anything else is dropped. |
| `keywords` | At least one must appear in subject or body. |
| `excluded_keywords` | If any appears (OTP, học phí, bảng điểm cá nhân ...) the email is dropped even from an allowed sender. |
| `newer_than_days`, `max_results` | Time window and how many emails to request. |
| `ingest_endpoint` | Server URL, currently `https://stimulus-rectangle-ship.ngrok-free.dev/api/inbox/batch`. |

## Run

Always run from the project root (`PythonAPI/`).

```bash
# Dry run: pull + filter + hash, print results, send nothing
.venv/bin/python3 run_ingestion.py

# One provider only
.venv/bin/python3 run_ingestion.py --source gmail
.venv/bin/python3 run_ingestion.py --source outlook

# Also send to the server (uses ingest_endpoint from the config)
.venv/bin/python3 run_ingestion.py --send

# Send to a different server
.venv/bin/python3 run_ingestion.py --send --server https://other-host/api/inbox/batch
```

Always do a dry run first and read the output before using `--send`; the server is shared.

## Outlook sign-in

Uses the Microsoft *device code* flow. When it starts, the script:
1. copies the one-time code to your clipboard,
2. opens `https://microsoft.com/devicelogin` in your browser,
3. you paste the code (Ctrl/Cmd+V), then sign in with your student account.

The token is cached in `outlook/msal_token_cache.json`, so later runs are silent.

**Known blocker:** HCMUS tenants may show **"Need admin approval"** for the app id used here.
That is a school policy, not a bug in the code; it needs an app registration approved by HCMUS IT.

## What gets sent to the server

Mapped to the server's `EmailIngestRequest`:

| Our field | Server field |
|---|---|
| `content_hash` (sender + first 30 words) | `message_id` |
| `from` | `sender` |
| `subject` | `subject` |
| `date` | `received_at` |
| `text` | `content` |
| (none yet) | `media_urls` = `[]` |
| `--client-id` not yet exposed; defaults to `anonymous` | `client_id` |
