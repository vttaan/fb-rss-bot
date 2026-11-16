import json
from datetime import datetime
from database import get_session, init_db, add_fanpage
from database.models import EventRecord, Fanpage, Post, PostAnalysis
from events.extractor import sync_all_event_folders, _write_events_json, upsert_unified_event

def run():
    init_db()

    with open("extracted_events.json", "r", encoding="utf-8") as f:
        extracted_data = json.load(f)

    title_fixes = {
        "1000608555828661": "🚀 [BUILD ON STELLAR] — FROM ZERO TO YOUR FIRST WEB3 APP 💻",
        "1622430336564651": "[GALA CHUNG KẾT SCHALLENGE 2026 - SẴN SÀNG BÙNG NỔ CHẶNG ĐUA CUỐI CÙNG] 🔥",
    }

    with get_session() as session:
        for item in extracted_data:
            if not item.get("is_event"):
                continue

            post_id = item.get("post_id")
            title = item.get("title")
            if title == "Untitled Event" and post_id in title_fixes:
                title = title_fixes[post_id]
                item["title"] = title

            fp_url = item.get("fanpage_url") or "https://www.facebook.com/unknown"
            fp_name = item.get("fanpage_name")

            try:
                fp, _ = add_fanpage(fp_url, fp_name)
            except Exception:
                slug = (fp_url or "").strip("/").split("/")[-1]
                fp = session.query(Fanpage).filter_by(slug=slug).first()
                if not fp:
                    fp = Fanpage(url=fp_url, slug=slug, name=fp_name or slug, is_active=True)
                    session.add(fp)
                    session.flush()

            existing_post = session.query(Post).filter_by(post_id=post_id).first()
            pub_dt = None
            if item.get("posted_at"):
                try:
                    pub_dt = datetime.fromisoformat(item["posted_at"])
                except Exception:
                    pub_dt = None

            if not existing_post:
                new_post = Post(
                    post_id=post_id,
                    fanpage_id=fp.id,
                    content=item.get("description") or "",
                    media_urls_json=json.dumps(item.get("media_urls") or []),
                    published_at=pub_dt,
                    post_url=item.get("post_url"),
                )
                session.add(new_post)
                session.flush()

            pa = session.query(PostAnalysis).filter_by(post_id=post_id).first()
            if not pa:
                pa = PostAnalysis(
                    post_id=post_id,
                    fanpage_id=fp.id,
                    is_event=True,
                    event_json=json.dumps(item, ensure_ascii=False),
                )
                session.add(pa)
                session.flush()

            raw_event_data = dict(item)
            upsert_status, ev_record, changes = upsert_unified_event(
                session=session,
                raw_data=raw_event_data,
                source_type="facebook",
                source_id=post_id,
                media_urls=item.get("media_urls") or [],
                fanpage_id=fp.id,
                client_id=None,
                post_url=item.get("post_url"),
                posted_at=item.get("posted_at"),
            )
            print(f"[{upsert_status}] {title} -> ID {ev_record.id}")

        session.commit()

        for fp in session.query(Fanpage).all():
            _write_events_json(fp, session)

        sync_all_event_folders(session)

        total_evs = session.query(EventRecord).count()
        print(f"\nSuccessfully populated database and feeds! Total EventRecord count: {total_evs}")

if __name__ == "__main__":
    run()
