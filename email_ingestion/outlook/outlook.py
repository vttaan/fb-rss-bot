import os
import sys
import subprocess
import webbrowser
from datetime import datetime, timedelta, timezone
import msal
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from mail_provider import MailProvider, DEFAULT_FILTER_FIELDS
from privacy_filter import extract_html_media_urls, load_filter_config, strip_html

THUNDERBIRD_CLIENT_ID = "9e5f94bc-e8a4-4e73-b8be-63364c29d753"
GRAPH_CLIENT_ID = THUNDERBIRD_CLIENT_ID
AUTHORITY = "https://login.microsoftonline.com/common"
GRAPH_SCOPES = ["https://graph.microsoft.com/Mail.Read"]
TOKEN_CACHE_PATH = os.path.join(BASE_DIR, "msal_token_cache.json")


def copy_to_clipboard(text):
    if sys.platform == "darwin":
        cmd = ["pbcopy"]
    elif sys.platform.startswith("win"):
        cmd = ["clip"]
    else:
        cmd = ["xclip", "-selection", "clipboard"]
    try:
        subprocess.run(cmd, input=text.encode("utf-8"), check=True)
        return True
    except Exception:
        return False


def _collapse_sender_domains(allowed_senders):
    """Collapse subdomains and addresses into minimal root domains for concise Graph KQL."""
    raw = [s.strip().lower() for s in (allowed_senders or []) if s and s.strip()]
    domains = sorted({s.split("@", 1)[1] if "@" in s else s for s in raw}, key=len)
    roots = []
    for d in domains:
        if not any(d == r or d.endswith("." + r) for r in roots):
            roots.append(d)
    return roots


def build_graph_search_query(allowed_senders=None, keywords=None, excluded_keywords=None, newer_than_days=14):
    """Build Microsoft Graph $search KQL parity query matching Gmail's 4 filter dimensions."""
    clauses = []

    roots = _collapse_sender_domains(allowed_senders)
    if roots:
        if len(roots) == 1:
            clauses.append(f"from:{roots[0]}")
        else:
            sender_terms = " OR ".join(f"from:{r}" for r in roots[:5])
            clauses.append(f"({sender_terms})")

    kw_list = [kw.strip() for kw in (keywords or []) if kw and str(kw).strip()][:12]
    if kw_list:
        escaped_terms = [f'\\"{kw}\\"' for kw in kw_list]
        kw_expr = " OR ".join(escaped_terms)
        clauses.append(
            f"({kw_expr})"
            if (roots or excluded_keywords or newer_than_days) and len(kw_list) > 1
            else kw_expr
        )

    excl_list = [ek.strip() for ek in (excluded_keywords or []) if ek and str(ek).strip()][:4]
    if excl_list:
        clauses.append(" AND ".join(f'NOT \\"{ek}\\"' for ek in excl_list))

    if newer_than_days and int(newer_than_days) > 0:
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=int(newer_than_days))).strftime("%Y-%m-%d")
        clauses.append(f"received>={cutoff_date}")

    if not clauses:
        return ""
    return f'"{" AND ".join(clauses)}"'


class OutlookProvider(MailProvider):
    """Outlook (Microsoft Graph) implementation of the generalized MailProvider interface."""

    provider_name = "outlook"

    def authenticate(self, credentials=None, restrictions=None):
        scopes = restrictions or GRAPH_SCOPES
        client_id = (credentials or {}).get("client_id", GRAPH_CLIENT_ID)
        authority = (credentials or {}).get("authority", AUTHORITY)
        cache_path = (credentials or {}).get("token_cache_path", TOKEN_CACHE_PATH)

        cache = msal.SerializableTokenCache()
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                cache.deserialize(f.read())

        app = msal.PublicClientApplication(
            client_id, authority=authority, token_cache=cache
        )

        accounts = app.get_accounts()
        result = None
        if accounts:
            result = app.acquire_token_silent(scopes, account=accounts[0])

        if not result:
            flow = app.initiate_device_flow(scopes=scopes)
            if "user_code" not in flow:
                raise ValueError(f"Failed to create device flow: {flow}")
            print(flow["message"])
            copied = copy_to_clipboard(flow["user_code"])
            if copied:
                print(f"[Outlook] Code {flow['user_code']} copied to clipboard - just paste it (Ctrl/Cmd+V).")
            webbrowser.open(flow["verification_uri"])
            result = app.acquire_token_by_device_flow(flow)

        if cache.has_state_changed:
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(cache.serialize())

        if "access_token" not in result:
            raise RuntimeError(f"Authentication failed: {result.get('error_description', result)}")

        return result["access_token"]

    def build_query(self, allowed_senders, keywords, excluded_keywords=None, newer_than_days=14):
        return build_graph_search_query(
            allowed_senders=allowed_senders,
            keywords=keywords,
            excluded_keywords=excluded_keywords,
            newer_than_days=newer_than_days,
        )

    def fetch_emails(self, connection, query, filter_fields=DEFAULT_FILTER_FIELDS, max_results=20):
        headers = {
            "Authorization": f"Bearer {connection}",
            "ConsistencyLevel": "eventual",
        }
        params = {
            "$top": max_results,
            "$select": "id,subject,from,receivedDateTime,bodyPreview,body",
        }
        if query:
            params["$search"] = query

        response = requests.get(
            "https://graph.microsoft.com/v1.0/me/messages",
            headers=headers,
            params=params,
            timeout=30,
        )
        if response.status_code == 400 and query and " AND " in query:
            inner = query.strip('"')
            kw_clauses = [
                p.strip()
                for p in inner.split(" AND ")
                if p.strip() and not p.strip().startswith(("from:", "(from:", "NOT ", "received>="))
            ]
            if kw_clauses:
                params["$search"] = f'"{kw_clauses[0].strip("()")}"'
            else:
                params.pop("$search", None)
            response = requests.get(
                "https://graph.microsoft.com/v1.0/me/messages",
                headers=headers,
                params=params,
                timeout=30,
            )
        response.raise_for_status()

        raw_emails = []
        for msg in response.json().get("value", []):
            sender_info = msg.get("from", {}).get("emailAddress", {})
            sender_addr = sender_info.get("address", "")
            sender_name = sender_info.get("name", "")
            from_formatted = f"{sender_name} <{sender_addr}>" if sender_name else sender_addr

            body_obj = msg.get("body", {})
            raw_content = body_obj.get("content", "") or msg.get("bodyPreview", "")
            is_html = body_obj.get("contentType", "").lower() == "html"
            clean_text = strip_html(raw_content) if is_html else raw_content
            media_urls = extract_html_media_urls(raw_content) if is_html else []
            raw_emails.append({
                "id": msg.get("id", ""),
                "from": from_formatted,
                "subject": msg.get("subject", ""),
                "date": msg.get("receivedDateTime", ""),
                "text": clean_text,
                "media_urls": media_urls,
            })
        return raw_emails


def fetch_drl_emails(config=None):
    """Backwards-compatible helper using OutlookProvider."""
    provider = OutlookProvider()
    return provider.ingest(config=config)


if __name__ == "__main__":
    emails = fetch_drl_emails()
    print(f"Found {len(emails)} privacy-verified DRL candidate email(s) from Outlook.")
    for mail in emails:
        print("-" * 60)
        print(f"From:    {mail['from']}")
        print(f"Subject: {mail['subject']}")
