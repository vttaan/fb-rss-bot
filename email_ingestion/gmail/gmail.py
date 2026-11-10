import os
import sys
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from mail_provider import MailProvider, DEFAULT_FILTER_FIELDS
from privacy_filter import load_filter_config, strip_html

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")


def build_gmail_query(allowed_senders=None, keywords=None, excluded_keywords=None, newer_than_days=14, extra_query=""):
    parts = []
    if allowed_senders:
        sender_terms = " OR ".join(f"from:{s}" for s in allowed_senders)
        parts.append(f"({sender_terms})")

    if keywords:
        kw_terms = " OR ".join(f'"{kw}"' for kw in keywords)
        parts.append(f"({kw_terms})")

    if excluded_keywords:
        excl_terms = " ".join(f'-"{kw}"' for kw in excluded_keywords)
        parts.append(excl_terms)

    if newer_than_days:
        parts.append(f"newer_than:{int(newer_than_days)}d")

    if extra_query:
        parts.append(extra_query)

    return " ".join(parts)


def get_body(payload):
    """Pull plain text out of a Gmail message payload (with HTML fallback)."""
    mime_type = payload.get("mimeType", "")
    body_data = payload.get("body", {}).get("data")

    if mime_type == "text/plain" and body_data:
        return base64.urlsafe_b64decode(body_data).decode("utf-8", "ignore")

    parts = payload.get("parts", [])
    for part in parts:
        if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
            return base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", "ignore")

    if mime_type == "text/html" and body_data:
        raw_html = base64.urlsafe_b64decode(body_data).decode("utf-8", "ignore")
        return strip_html(raw_html)

    for part in parts:
        text = get_body(part)
        if text:
            return text
    return ""


class GmailProvider(MailProvider):
    """Gmail implementation of the generalized MailProvider interface."""

    provider_name = "gmail"

    def authenticate(self, credentials=None, restrictions=None):
        scopes = restrictions or SCOPES
        creds_file = (credentials or {}).get("credentials_path", CREDENTIALS_PATH)
        token_file = (credentials or {}).get("token_path", TOKEN_PATH)

        creds = None
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, scopes)
                creds = flow.run_local_server(port=0)
            with open(token_file, "w", encoding="utf-8") as f:
                f.write(creds.to_json())
        return build("gmail", "v1", credentials=creds)

    def build_query(self, allowed_senders, keywords, excluded_keywords=None, newer_than_days=14):
        return build_gmail_query(
            allowed_senders=allowed_senders,
            keywords=keywords,
            excluded_keywords=excluded_keywords,
            newer_than_days=newer_than_days,
        )

    def fetch_emails(self, connection, query, filter_fields=DEFAULT_FILTER_FIELDS, max_results=20):
        res = connection.users().messages().list(
            userId="me", q=query, maxResults=max_results
        ).execute()

        raw_emails = []
        for m in res.get("messages", []):
            msg = connection.users().messages().get(
                userId="me", id=m["id"], format="full"
            ).execute()
            headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
            raw_emails.append({
                "id": msg["id"],
                "from": headers.get("from", ""),
                "subject": headers.get("subject", ""),
                "date": headers.get("date", ""),
                "text": get_body(msg.get("payload", {})) or msg.get("snippet", ""),
            })
        return raw_emails


def fetch_drl_emails(config=None, extra_query=""):
    """Backwards-compatible helper using GmailProvider."""
    provider = GmailProvider()
    return provider.ingest(config=config)


if __name__ == "__main__":
    emails = fetch_drl_emails()
    print(f"Found {len(emails)} privacy-verified DRL candidate email(s) from Gmail.")
    for mail in emails:
        print("-" * 60)
        print(f"Hash:     {mail['content_hash'][:16]}...")
        print(f"From:     {mail['from']}")
        print(f"Subject:  {mail['subject']}")
