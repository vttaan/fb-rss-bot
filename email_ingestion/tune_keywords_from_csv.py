"""
Scan a labeled n-gram frequency CSV (e.g. data/post_word_frequency_v4.csv)
to audit existing filter_config.json keywords/non_event_keywords and suggest
high-precision additions. Dry-run by default; pass --apply to update config.
"""

import argparse
import csv
import json
import os
import unicodedata
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV_PATH = os.path.join(BASE_DIR, "data", "post_word_frequency_v4.csv")
DEFAULT_CLIENT_CONFIG = os.path.join(BASE_DIR, "filter_config.json")
DEFAULT_SERVER_CONFIG = os.path.normpath(
    os.path.join(BASE_DIR, "..", "scrape_server", "feeds", "config", "filter_config.json")
)


def _norm(text: str) -> str:
    """Normalize to NFC lowercase for consistent Vietnamese phrase matching."""
    return unicodedata.normalize("NFC", (text or "").strip()).lower()


def load_csv_rows(csv_path: str):
    """Load n-gram rows from the word-frequency CSV with numeric fields parsed."""
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            phrase = (raw.get("phrase") or "").strip()
            if not phrase:
                continue
            try:
                rows.append({
                    "rank": int(raw.get("rank") or 0),
                    "ngram_size": int(raw.get("ngram_size") or 0),
                    "phrase": phrase,
                    "norm_phrase": _norm(phrase),
                    "total_count": int(raw.get("total_count") or 0),
                    "doc_count": int(raw.get("doc_count") or 0),
                    "doc_pct": float(raw.get("doc_pct") or 0.0),
                    "event_count": int(raw.get("event_count") or 0),
                    "event_pct": float(raw.get("event_pct") or 0.0),
                    "non_event_count": int(raw.get("non_event_count") or 0),
                    "non_event_pct": float(raw.get("non_event_pct") or 0.0),
                })
            except ValueError:
                continue
    return rows


def _is_redundant_with(phrase_norm: str, existing_norms: set) -> bool:
    """True if phrase overlaps an existing configured term (exact, sub-fragment, or super-phrase)."""
    if phrase_norm in existing_norms:
        return True
    for ex in existing_norms:
        if ex and (ex in phrase_norm or phrase_norm in ex):
            return True
    return False


def suggest_candidates(
    rows,
    existing_keywords,
    existing_non_events,
    min_ngram=2,
    min_pos_docs=8,
    min_pos_pct=90.0,
    min_neg_docs=5,
    min_neg_pct=70.0,
    top_n=15,
):
    """Return (positive_candidates, non_event_candidates) sorted by count & precision."""
    pos_existing = {_norm(k) for k in existing_keywords}
    neg_existing = {_norm(k) for k in existing_non_events}

    pos_candidates = []
    neg_candidates = []
    for r in rows:
        if r["ngram_size"] < min_ngram:
            continue
        if r["doc_count"] >= min_pos_docs and r["event_pct"] >= min_pos_pct:
            if not _is_redundant_with(r["norm_phrase"], pos_existing):
                pos_candidates.append(r)
        if r["non_event_count"] >= min_neg_docs and r["non_event_pct"] >= min_neg_pct:
            if not _is_redundant_with(r["norm_phrase"], neg_existing):
                neg_candidates.append(r)

    pos_candidates.sort(key=lambda r: (r["event_pct"], r["event_count"], -r["ngram_size"]), reverse=True)
    neg_candidates.sort(key=lambda r: (r["non_event_pct"], r["non_event_count"], -r["ngram_size"]), reverse=True)
    return pos_candidates[:top_n], neg_candidates[:top_n]


def audit_existing(rows, terms, label, warn_below_pct, pct_field):
    """Print CSV stats for each configured term and flag noisy ones."""
    by_norm = {}
    for r in rows:
        by_norm.setdefault(r["norm_phrase"], r)

    print(f"\n=== Audit: {label} ({len(terms)} configured) ===")
    for term in terms:
        row = by_norm.get(_norm(term))
        if not row:
            print(f"  - {term!r}: not found in CSV n-grams")
            continue
        pct = row[pct_field]
        flag = " [LOW PRECISION]" if pct < warn_below_pct else ""
        print(
            f"  - {term!r}: docs={row['doc_count']} "
            f"(event={row['event_count']}/{row['event_pct']:.1f}%, "
            f"non_event={row['non_event_count']}/{row['non_event_pct']:.1f}%){flag}"
        )


def _append_unique(existing, additions):
    seen = {_norm(x) for x in existing}
    updated = list(existing)
    added = []
    for item in additions:
        if _norm(item) not in seen:
            seen.add(_norm(item))
            updated.append(item)
            added.append(item)
    return updated, added


def apply_updates(config_path, add_keywords, add_non_events, bump_version=False):
    """Write newly accepted keywords/non_event_keywords into a filter_config.json file."""
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    cfg["keywords"], added_kw = _append_unique(cfg.get("keywords", []), add_keywords)
    cfg["non_event_keywords"], added_neg = _append_unique(
        cfg.get("non_event_keywords", []), add_non_events
    )
    if bump_version and (added_kw or added_neg):
        cfg["version"] = int(cfg.get("version", 1)) + 1
        cfg["updated_at"] = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return added_kw, added_neg, cfg.get("version")


def main():
    parser = argparse.ArgumentParser(description="Audit and tune filter_config.json from word-frequency CSV.")
    parser.add_argument("--csv", default=DEFAULT_CSV_PATH, help="Path to post_word_frequency CSV")
    parser.add_argument("--config", default=DEFAULT_CLIENT_CONFIG, help="Path to client filter_config.json")
    parser.add_argument("--server-config", default=DEFAULT_SERVER_CONFIG, help="Path to server filter_config.json")
    parser.add_argument("--min-ngram", type=int, default=2, help="Min n-gram size for candidates (default: 2)")
    parser.add_argument("--min-pos-docs", type=int, default=8, help="Min doc_count for positive candidates")
    parser.add_argument("--min-pos-pct", type=float, default=90.0, help="Min event_pct for positive candidates")
    parser.add_argument("--min-neg-docs", type=int, default=5, help="Min non_event_count for disqualifiers")
    parser.add_argument("--min-neg-pct", type=float, default=70.0, help="Min non_event_pct for disqualifiers")
    parser.add_argument("--top", type=int, default=15, help="Max candidates to show per category")
    parser.add_argument("--max-add", type=int, default=5, help="Max candidates per category to add with --apply")
    parser.add_argument("--apply", action="store_true", help="Append top --max-add candidates to --config")
    parser.add_argument("--sync-server", action="store_true", help="Also update --server-config and bump version")
    args = parser.parse_args()

    rows = load_csv_rows(args.csv)
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    existing_kw = cfg.get("keywords", [])
    existing_neg = cfg.get("non_event_keywords", [])

    print(f"Loaded {len(rows)} n-grams from {args.csv}")
    audit_existing(rows, existing_kw, "keywords", warn_below_pct=75.0, pct_field="event_pct")
    audit_existing(rows, existing_neg, "non_event_keywords", warn_below_pct=60.0, pct_field="non_event_pct")

    pos_cands, neg_cands = suggest_candidates(
        rows,
        existing_kw,
        existing_neg,
        min_ngram=args.min_ngram,
        min_pos_docs=args.min_pos_docs,
        min_pos_pct=args.min_pos_pct,
        min_neg_docs=args.min_neg_docs,
        min_neg_pct=args.min_neg_pct,
        top_n=args.top,
    )

    print(f"\n=== Top {len(pos_cands)} Positive Keyword Candidates (docs>={args.min_pos_docs}, event_pct>={args.min_pos_pct}%) ===")
    for r in pos_cands:
        print(f"  + {r['phrase']!r} (n={r['ngram_size']}, docs={r['doc_count']}, event={r['event_count']}/{r['event_pct']:.1f}%)")

    print(f"\n=== Top {len(neg_cands)} Non-Event Disqualifier Candidates (non_event>={args.min_neg_docs}, non_event_pct>={args.min_neg_pct}%) ===")
    for r in neg_cands:
        print(f"  - {r['phrase']!r} (n={r['ngram_size']}, docs={r['doc_count']}, non_event={r['non_event_count']}/{r['non_event_pct']:.1f}%)")

    if not args.apply:
        print("\nDry-run only. Re-run with --apply (and optionally --sync-server) to write changes.")
        return

    to_add_pos = [r["phrase"] for r in pos_cands[:args.max_add]]
    to_add_neg = [r["phrase"] for r in neg_cands[:args.max_add]]

    added_kw, added_neg, ver = apply_updates(args.config, to_add_pos, to_add_neg, bump_version=args.sync_server)
    print(f"\nUpdated {args.config} (version={ver}): +{len(added_kw)} keywords, +{len(added_neg)} non_event_keywords")

    if args.sync_server and os.path.exists(args.server_config):
        s_kw, s_neg, s_ver = apply_updates(args.server_config, to_add_pos, to_add_neg, bump_version=True)
        print(f"Updated {args.server_config} (version={s_ver}): +{len(s_kw)} keywords, +{len(s_neg)} non_event_keywords")


if __name__ == "__main__":
    main()
