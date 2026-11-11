"""
Client Email Ingestion Runner & HTTP Shipper.

Role in Architecture Diagram:
  Ingestion Stream 2: Clients' Emails (Client A / Client B)
  -> Authenticates, builds query, pulls emails
  -> 3-Stage privacy and keyword gate
  -> Generates deterministic hash (sender + first 30 words)
  -> HTTP POST JSON to FastAPI Route: `/api/events/ingest-email`
     (via localhost:8000 or ngrok public gateway)
"""

import sys
import json
import argparse
import requests

from privacy_filter import load_filter_config, sync_filter_config_from_server
from gmail.gmail import GmailProvider
from outlook.outlook import OutlookProvider


def to_server_schema(email_item, client_id="anonymous"):
    """Map an internal verified email dict into the server's EmailIngestRequest schema."""
    return {
        "client_id": client_id,
        "message_id": email_item.get("message_id") or email_item["content_hash"],
        "content_hash": email_item["content_hash"],
        "subject": email_item.get("subject", ""),
        "sender": email_item.get("from", ""),
        "received_at": email_item.get("date", ""),
        "content": email_item.get("text", ""),
        "media_urls": [],
    }


def _derive_check_url(endpoint_url):
    """Derive `/api/inbox/check` from `/api/inbox/batch`, `/api/inbox`, or `/api/events/ingest-email`."""
    for suffix in ("/api/inbox/batch", "/api/inbox", "/api/events/ingest-email"):
        if endpoint_url.endswith(suffix):
            return endpoint_url[: -len(suffix)] + "/api/inbox/check"
    return None


def filter_unseen_by_server(emails, endpoint_url, headers=None, timeout=15):
    """
    Pre-flight hash check (`POST /api/inbox/check`):
    Sends only `content_hash` values first so email bodies already known to the server
    never leave the student's machine. Falls back gracefully if the server doesn't
    implement `/api/inbox/check` yet.
    """
    check_url = _derive_check_url(endpoint_url)
    if not check_url or not emails:
        return emails

    hashes = [m["content_hash"] for m in emails if m.get("content_hash")]
    try:
        res = requests.post(check_url, json={"hashes": hashes}, headers=headers or {}, timeout=timeout)
        if res.status_code == 404:
            return emails  # Older server without /api/inbox/check
        if res.ok:
            data = res.json()
            known = set(data.get("known") or [])
            if known:
                print(f"[Shipper] Pre-flight hash check: {len(known)}/{len(emails)} email(s) already on server; skipping their bodies.")
            return [m for m in emails if m.get("content_hash") not in known]
    except Exception as exc:
        print(f"[Shipper] Pre-flight check skipped ({exc}); proceeding with full send.")
    return emails


def ship_to_server(emails, endpoint_url, client_id="anonymous", ingest_token="", timeout=30):
    """
    Ships filtered and hashed email events via HTTP POST JSON to the FastAPI route.
    """
    if not endpoint_url:
        print("[Shipper] Error: No endpoint URL specified.")
        return False, None

    if not emails:
        print("[Shipper] No candidate emails to ship.")
        return True, None

    headers = {}
    token = (ingest_token or "").strip()
    if token:
        headers["X-Ingest-Token"] = token

    unseen_emails = filter_unseen_by_server(emails, endpoint_url, headers=headers)
    if not unseen_emails:
        print("[Shipper] All candidate emails are already recorded on the server. Nothing to upload.")
        return True, None

    payload = {
        "emails": [to_server_schema(m, client_id=client_id) for m in unseen_emails]
    }

    print(f"\n[Shipper] Sending {len(unseen_emails)} unseen email(s) via HTTP POST to: {endpoint_url}")
    try:
        response = requests.post(endpoint_url, json=payload, headers=headers, timeout=timeout)
        print(f"          Response Status: {response.status_code}")
        try:
            resp_body = response.json()
            print(f"          Response JSON:   {json.dumps(resp_body, indent=2, ensure_ascii=False)}")
        except Exception:
            print(f"          Response Body:   {response.text[:300]}")
        return response.ok, response
    except Exception as e:
        print(f"          [Error] Connection failed: {e}")
        return False, None


def run_pipeline(sources=("gmail", "outlook"), send_to_server=False, server_url=None, verbose=True, sync_config=True):
    if sync_config:
        config = sync_filter_config_from_server()
    else:
        config = load_filter_config()
    target_endpoint = server_url or config.get("ingest_endpoint")

    collected_emails = []

    # 1. Fetch from Gmail
    if "gmail" in sources:
        print("\n" + "=" * 65)
        print(">>> [Ingestion Stream 2] Fetching from Gmail...")
        try:
            gmail_provider = GmailProvider()
            emails = gmail_provider.ingest(config=config)
            print(f"    Fetched {len(emails)} privacy-verified DRL email(s) from Gmail.")
            collected_emails.extend(emails)
        except Exception as e:
            print(f"    [Error] Gmail ingestion failed: {e}")

    # 2. Fetch from Outlook
    if "outlook" in sources:
        print("\n" + "=" * 65)
        print(">>> [Ingestion Stream 2] Fetching from Outlook...")
        try:
            outlook_provider = OutlookProvider()
            emails = outlook_provider.ingest(config=config)
            print(f"    Fetched {len(emails)} privacy-verified DRL email(s) from Outlook.")
            collected_emails.extend(emails)
        except Exception as e:
            print(f"    [Error] Outlook ingestion failed: {e}")

    # 3. Client-side local deduplication by content_hash (sender + first 30 words)
    unique_emails = {}
    for mail in collected_emails:
        h = mail["content_hash"]
        if h not in unique_emails:
            unique_emails[h] = mail

    final_emails = list(unique_emails.values())
    print("\n" + "=" * 65)
    print(f">>> Summary: {len(final_emails)} verified email(s) ready to ship.")

    if verbose:
        for idx, mail in enumerate(final_emails, 1):
            print(f"\n[{idx}] [{mail['source'].upper()}] {mail['subject']}")
            print(f"    From:        {mail['from']}")
            print(f"    Date:        {mail['date']}")
            print(f"    Hash:        {mail['content_hash']}")
            print(f"    Hash Source: {mail.get('hash_source', '')[:80]}...")
            print(f"    Keywords:    {mail['matched_keywords']}")
            preview = mail['text'][:120].replace('\n', ' ')
            print(f"    Preview:     {preview}...")

    # 4. Ship to server via HTTP POST JSON
    if send_to_server:
        print("\n" + "=" * 65)
        ship_to_server(
            final_emails,
            target_endpoint,
            client_id=config.get("client_id", "anonymous"),
            ingest_token=config.get("ingest_api_key", ""),
        )

    return final_emails


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client Email Ingestion & Shipping CLI")
    parser.add_argument("--source", choices=["gmail", "outlook", "both"], default="both",
                        help="Which provider(s) to pull from (default: both)")
    parser.add_argument("--send", action="store_true",
                        help="HTTP POST JSON to the server's /api/events/ingest-email")
    parser.add_argument("--server", type=str, default=None,
                        help="Override target server endpoint URL (e.g. ngrok gateway URL)")
    parser.add_argument("--no-config-sync", action="store_true",
                        help="Disable pulling latest filter_config.json from server before running")
    args = parser.parse_args()

    selected_sources = ("gmail", "outlook") if args.source == "both" else (args.source,)
    run_pipeline(
        sources=selected_sources,
        send_to_server=args.send,
        server_url=args.server,
        sync_config=not args.no_config_sync
    )
