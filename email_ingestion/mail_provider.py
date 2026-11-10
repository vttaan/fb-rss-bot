"""
Generalized Mail Ingestion Interface (`MailProvider`).

Follows the unified architecture diagram:
  1. authenticate(credentials, restrictions) -> session / connection
  2. build_query(allowed_senders, keywords, excluded_keywords, newer_than_days) -> query_str
  3. fetch_emails(connection, query, filter_fields, max_results) -> list[RawEmail]
  4. normalize_email(raw_msg, filter_fields) -> RawEmail
  5. filter_privacy_and_keywords(emails, config) -> list[VerifiedEmail]
  6. ingest(config) -> list[VerifiedEmail] (Template method shared across Gmail, Outlook, etc.)
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from privacy_filter import (
    load_filter_config,
    passes_privacy_and_keyword_gate,
    format_for_llm_server,
)


DEFAULT_FILTER_FIELDS = ("id", "from", "subject", "date", "body")


class MailProvider(ABC):
    """Abstract base class generalizing Gmail, Outlook Graph, Outlook IMAP, or Web-Tab Providers."""

    provider_name: str = "base"

    @abstractmethod
    def authenticate(self, credentials: Optional[Dict[str, Any]] = None, restrictions: Optional[List[str]] = None) -> Any:
        """
        Establish an authenticated API connection or session token.
        - Input: provider credentials path/dict, OAuth scopes/restrictions
        - Output: established API connection / client / token
        """
        pass

    @abstractmethod
    def build_query(
        self,
        allowed_senders: List[str],
        keywords: List[str],
        excluded_keywords: Optional[List[str]] = None,
        newer_than_days: int = 14,
    ) -> str:
        """
        Translate provider-agnostic filter rules into a provider-specific server query
        (e.g., Gmail `q` string or Microsoft Graph `$search` KQL).
        """
        pass

    @abstractmethod
    def fetch_emails(
        self,
        connection: Any,
        query: str,
        filter_fields: tuple = DEFAULT_FILTER_FIELDS,
        max_results: int = 20,
    ) -> List[Dict[str, str]]:
        """
        Fetch raw messages from the provider using `connection` and `query`,
        returning a list of normalized dictionaries with keys matching `filter_fields`:
          {"id": ..., "from": ..., "subject": ..., "date": ..., "text": ...}
        """
        pass

    def filter_privacy_and_keywords(
        self, raw_emails: List[Dict[str, str]], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Apply the 3-stage local privacy & keyword gate and attach `content_hash`
        across all providers uniformly.
        """
        verified = []
        for mail in raw_emails:
            passed, reason, matched_kw = passes_privacy_and_keyword_gate(
                from_addr=mail.get("from", ""),
                subject=mail.get("subject", ""),
                body_text=mail.get("text", ""),
                config=config,
            )
            if not passed:
                continue

            verified.append(
                format_for_llm_server(
                    source=self.provider_name,
                    msg_id=mail.get("id", ""),
                    from_str=mail.get("from", ""),
                    subject=mail.get("subject", ""),
                    date_str=mail.get("date", ""),
                    body_text=mail.get("text", ""),
                    matched_keywords=matched_kw,
                )
            )
        return verified

    def ingest(
        self,
        config: Optional[Dict[str, Any]] = None,
        filter_fields: tuple = DEFAULT_FILTER_FIELDS,
    ) -> List[Dict[str, Any]]:
        """
        End-to-end template pipeline for any mail provider:
          authenticate -> build_query -> fetch_emails -> filter_privacy_and_keywords
        """
        if config is None:
            config = load_filter_config()

        conn = self.authenticate()
        query = self.build_query(
            allowed_senders=config.get("allowed_senders", []),
            keywords=config.get("keywords", []),
            excluded_keywords=config.get("excluded_keywords", []),
            newer_than_days=config.get("newer_than_days", 14),
        )
        raw_emails = self.fetch_emails(
            connection=conn,
            query=query,
            filter_fields=filter_fields,
            max_results=config.get("max_results", 20),
        )
        return self.filter_privacy_and_keywords(raw_emails, config)

