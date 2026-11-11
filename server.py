import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from config import (
    DEFAULT_INTERVAL_SECONDS,
    DEFAULT_SCRAPE_HOURS,
    DEFAULT_SCRAPE_MINUTE,
    FEEDS_DIR,
    LOCAL_TIMEZONE,
    PUBLIC_NGROK_URL,
    SCRAPE_MISFIRE_GRACE_SECONDS,
    SCRAPE_TIMEZONE,
)
from database import (
    Fanpage,
    Post,
    get_all_events,
    get_all_recent_posts,
    get_inbox_messages,
    get_or_create_user,
    get_posts_for_user,
    get_subscribers_for_fanpage,
    get_user_subscriptions,
    init_db,
    list_fanpages,
    subscribe_user_to_fanpages,
    unsubscribe_user_from_fanpage,
)
from events import extract_single_post_url, ingest_email_message, sync_all_event_folders
from rss import generate_user_rss_xml
from scheduler import backup_state_to_git, build_daily_cron_trigger, parse_scrape_hours, run_scrape_cycle


class SubscribeRequest(BaseModel):
    urls: List[str]


class ExtractPostRequest(BaseModel):
    url: str = Field(..., description="Public Facebook post URL to scrape and analyze")


class EmailIngestRequest(BaseModel):
    client_id: str = Field("anonymous", description="Client or user identifier submitting the email")
    message_id: Optional[str] = Field(None, description="Unique Email Message-ID header or UID")
    subject: str = Field("", description="Email subject line")
    sender: Optional[str] = Field(None, description="Sender email address or display name")
    received_at: Optional[str] = Field(None, description="ISO 8601 timestamp when email was received")
    content: str = Field(..., description="Plain-text email body content")
    media_urls: List[str] = Field(default_factory=list, description="Optional poster/attachment image URLs")


class BatchEmailIngestRequest(BaseModel):
    emails: List[EmailIngestRequest]


class ConnectionManager:
    """Manages active WebSocket connections mapped by user `client_id`."""

    def __init__(self) -> None:
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.scrape_lock = asyncio.Lock()

    async def connect(self, client_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.setdefault(client_id, set()).add(websocket)

    def disconnect(self, client_id: str, websocket: WebSocket) -> None:
        if client_id in self.active_connections:
            self.active_connections[client_id].discard(websocket)
            if not self.active_connections[client_id]:
                del self.active_connections[client_id]

    async def send_to_user(self, client_id: str, payload: dict) -> None:
        sockets = list(self.active_connections.get(client_id, []))
        dead_sockets: List[WebSocket] = []
        for ws in sockets:
            try:
                await ws.send_json(payload)
            except Exception:
                dead_sockets.append(ws)
        for ws in dead_sockets:
            self.disconnect(client_id, ws)

    async def broadcast_to_all(self, payload: dict) -> None:
        for client_id in list(self.active_connections.keys()):
            await self.send_to_user(client_id, payload)

    async def on_new_posts_scraped(self, fanpage: Fanpage, new_posts: List[Post]) -> None:
        """
        Callback invoked by the scraper whenever new posts are saved for a fanpage.
        Pushes new posts to public dashboard listeners and to subscribed users.
        """
        payload = {
            "event": "new_posts",
            "fanpage": fanpage.to_dict(),
            "posts": [p.to_dict() for p in new_posts],
        }
        await self.send_to_user("__public__", payload)

        subscriber_ids = get_subscribers_for_fanpage(fanpage.id)
        for client_id in subscriber_ids:
            if client_id in self.active_connections:
                generate_user_rss_xml(client_id)
                await self.send_to_user(client_id, payload)


manager = ConnectionManager()
scheduler = AsyncIOScheduler(timezone=SCRAPE_TIMEZONE)
SERVER_INTERVAL_SECONDS: Optional[int] = None
SERVER_SCRAPE_HOURS: str = DEFAULT_SCRAPE_HOURS
SERVER_SCRAPE_MINUTE: int = DEFAULT_SCRAPE_MINUTE
SERVER_AUTO_SCRAPE: bool = True


async def scheduled_scrape_task() -> None:
    if manager.scrape_lock.locked():
        return
    async with manager.scrape_lock:
        await manager.broadcast_to_all({"event": "status", "message": "Running scheduled fanpage scrape & extractor..."})
        new_count = await run_scrape_cycle(on_new_posts=manager.on_new_posts_scraped)
        await asyncio.to_thread(backup_state_to_git, "scheduled VM crawl")
        await manager.broadcast_to_all(
            {
                "event": "status",
                "message": f"Scrape cycle finished ({new_count} new post(s) discovered).",
                "refresh_tree": True,
            }
        )


async def trigger_background_scrape(target_slug_or_id: str | None = None) -> None:
    async with manager.scrape_lock:
        await manager.broadcast_to_all(
            {"event": "status", "message": f"Scraping fanpage(s) ({target_slug_or_id or 'all'})..."}
        )
        new_count = await run_scrape_cycle(
            target_slug_or_id=target_slug_or_id,
            on_new_posts=manager.on_new_posts_scraped,
        )
        await asyncio.to_thread(backup_state_to_git, "manual VM crawl")
        await manager.broadcast_to_all(
            {
                "event": "status",
                "message": f"Scrape finished! Found {new_count} new post(s).",
                "refresh_tree": True,
            }
        )


async def trigger_unified_scrape(target_slug_or_id: str | None = None, *, force_local: bool = False) -> Dict[str, Any]:
    """
    Unified scrape entrypoint for Ngrok web UI & APIs:
    - By default (`SERVER_AUTO_SCRAPE=True`), runs Playwright + Event Extractor directly on the VM
      and backs up `rss_bot.db` + `feeds/` to GitHub automatically.
    - If `--no-scrape` (`SERVER_AUTO_SCRAPE=False`) or `USE_GITHUB_ACTIONS_CRAWLER=true` is set,
      dispatches the GitHub Actions workflow (`live_server_demo.yml`) instead.
    """
    from admin import _trigger_github_actions_crawl

    use_actions_env = os.environ.get("USE_GITHUB_ACTIONS_CRAWLER", "").strip().lower() in ("1", "true", "yes")
    prefer_actions = (not force_local) and (not SERVER_AUTO_SCRAPE or use_actions_env)

    if prefer_actions:
        dispatched = await asyncio.to_thread(_trigger_github_actions_crawl, target_slug_or_id)
        if dispatched:
            scope = f" for '{target_slug_or_id}'" if target_slug_or_id else ""
            msg = f"Dispatched GitHub Actions crawler{scope} (results will sync back automatically)."
            await manager.broadcast_to_all({"event": "status", "message": msg})
            return {"status": "dispatched", "mode": "github_actions_dispatched", "message": msg}

        if not SERVER_AUTO_SCRAPE:
            msg = (
                "GitHub Actions dispatch unavailable (set GITHUB_TOKEN on the Ngrok host); "
                "queued for the next scheduled GitHub Actions crawl."
            )
            await manager.broadcast_to_all({"event": "status", "message": msg})
            return {"status": "queued_for_actions", "mode": "queued_for_actions", "message": msg}

    if manager.scrape_lock.locked():
        msg = "A local scrape cycle is already in progress..."
        return {"status": "busy", "mode": "local_background", "message": msg}

    asyncio.create_task(trigger_background_scrape(target_slug_or_id))
    msg = f"Started VM scrape & extraction ({target_slug_or_id or 'all'})."
    return {"status": "started", "mode": "local_background", "message": msg}


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await asyncio.to_thread(sync_all_event_folders)
    if SERVER_AUTO_SCRAPE:
        if SERVER_INTERVAL_SECONDS is not None:
            scheduler.add_job(
                scheduled_scrape_task,
                trigger="interval",
                seconds=SERVER_INTERVAL_SECONDS,
                id="server_scheduled_scrape",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=SCRAPE_MISFIRE_GRACE_SECONDS,
            )
        else:
            scheduler.add_job(
                scheduled_scrape_task,
                trigger=build_daily_cron_trigger(hours_str=SERVER_SCRAPE_HOURS, minute=SERVER_SCRAPE_MINUTE),
                id="server_scheduled_scrape",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=SCRAPE_MISFIRE_GRACE_SECONDS,
            )
        scheduler.start()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="Facebook Fanpage & Email Event Extractor Server",
    description=(
        "Scrapes public Facebook fanpages, receives client email entries via Inbox API, "
        "runs unified event extraction + deduplication/update, and distributes JSON/XML feeds by folders."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


from admin import router as admin_router  # noqa: E402

app.include_router(admin_router)


# --- Directory Tree Helper ---


def _inspect_feed_file(file_path: Path) -> Dict[str, Any]:
    rel = file_path.relative_to(FEEDS_DIR).as_posix()
    stat = file_path.stat()
    mtime_iso = datetime.fromtimestamp(stat.st_mtime, tz=LOCAL_TIMEZONE).isoformat()
    item_count: Optional[int] = None
    if file_path.suffix.lower() == ".json":
        try:
            doc = json.loads(file_path.read_text(encoding="utf-8"))
            if isinstance(doc, dict):
                item_count = doc.get("count")
        except Exception:
            pass
    return {
        "name": file_path.name,
        "path": f"/feeds/{rel}",
        "relative_path": rel,
        "type": file_path.suffix.lstrip(".").lower() or "file",
        "size_bytes": stat.st_size,
        "updated_at": mtime_iso,
        "count": item_count,
    }


def _build_feeds_directory_tree() -> Dict[str, Any]:
    """
    Build a structured directory tree of `feeds/` organized into logical folders so clients
    can easily browse and fetch extracted JSON and XML files.
    """
    sync_all_event_folders()
    folders: Dict[str, List[Dict[str, Any]]] = {
        "all": [],
        "fanpages": [],
        "emails": [],
        "inbox": [],
        "rss": [],
    }

    for p in sorted(FEEDS_DIR.rglob("*")):
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".json", ".xml"):
            continue
        rel_parts = p.relative_to(FEEDS_DIR).parts
        info = _inspect_feed_file(p)

        if rel_parts[0] == "all":
            folders["all"].append(info)
        elif rel_parts[0] == "fanpages":
            folders["fanpages"].append(info)
        elif rel_parts[0] == "emails":
            folders["emails"].append(info)
        elif rel_parts[0] == "inbox":
            folders["inbox"].append(info)
        elif len(rel_parts) == 1 and p.suffix.lower() == ".xml":
            folders["rss"].append(info)
            if p.name == "all.xml":
                folders["all"].append(info)
        elif len(rel_parts) == 2 and p.name == "events.json":
            # Legacy `feeds/<slug>/events.json` is already mirrored under `feeds/fanpages/<slug>/events.json`
            continue

    return {
        "public_url": PUBLIC_NGROK_URL,
        "updated_at": datetime.now(LOCAL_TIMEZONE).isoformat(),
        "folders": [
            {
                "folder": "/feeds/all/",
                "key": "all",
                "description": "Combined deduplicated events.json & all.xml across all sources",
                "files": folders["all"],
            },
            {
                "folder": "/feeds/fanpages/",
                "key": "fanpages",
                "description": "Extracted events.json distributed per Facebook fanpage folder (<slug>/events.json)",
                "files": folders["fanpages"],
            },
            {
                "folder": "/feeds/emails/",
                "key": "emails",
                "description": "Extracted events.json from client email submissions (global & per-client folders)",
                "files": folders["emails"],
            },
            {
                "folder": "/feeds/inbox/",
                "key": "inbox",
                "description": "Client Email Inbox messages log and extraction statuses",
                "files": folders["inbox"],
            },
            {
                "folder": "/feeds/rss/",
                "key": "rss",
                "description": "Raw RSS 2.0 XML feeds for fanpages and subscribed users",
                "files": folders["rss"],
            },
        ],
    }


# --- REST API Endpoints ---


@app.get("/api/tree")
def api_get_directory_tree():
    """Return the complete folder tree of distributed JSON and XML files for clients to discover & fetch."""
    return _build_feeds_directory_tree()


@app.get("/api/schedule")
def api_get_schedule():
    """Return the active scrape schedule configuration and next run timestamp."""
    job = scheduler.get_job("server_scheduled_scrape") if scheduler.running else None
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    parsed_hours = parse_scrape_hours(SERVER_SCRAPE_HOURS)
    mode = (
        "external_push"
        if not SERVER_AUTO_SCRAPE
        else ("interval" if SERVER_INTERVAL_SECONDS is not None else "daily_cron")
    )
    return {
        "mode": mode,
        "auto_scrape": SERVER_AUTO_SCRAPE,
        "timezone": str(SCRAPE_TIMEZONE),
        "interval_seconds": SERVER_INTERVAL_SECONDS,
        "scheduled_times_ict": [f"{h:02d}:{SERVER_SCRAPE_MINUTE:02d}" for h in parsed_hours],
        "next_run_at": next_run,
    }


@app.post("/api/inbox")
@app.post("/api/events/ingest-email")
async def api_ingest_email(body: EmailIngestRequest):
    """
    Inbox endpoint to receive an event-related email entry from a client, move it to the
    Unified Extractor, compare anchor fields (title, format online/offline, date, time, location)
    for deduplication or event update, gather remaining event details, and update JSON folders.
    """
    if not body.content or not body.content.strip():
        raise HTTPException(status_code=400, detail="Email content must not be empty.")

    get_or_create_user(body.client_id or "anonymous")
    result = await asyncio.to_thread(
        ingest_email_message,
        content=body.content,
        subject=body.subject,
        client_id=body.client_id,
        message_id=body.message_id,
        sender=body.sender,
        received_at=body.received_at,
        media_urls=body.media_urls,
    )

    await manager.broadcast_to_all(
        {
            "event": "inbox_updated",
            "result": result,
            "refresh_tree": True,
        }
    )
    return result


@app.post("/api/inbox/batch")
async def api_ingest_email_batch(body: BatchEmailIngestRequest):
    """Batch upload endpoint for clients submitting multiple filtered emails at once."""
    results = []
    for item in body.emails:
        if not item.content or not item.content.strip():
            continue
        get_or_create_user(item.client_id or "anonymous")
        res = await asyncio.to_thread(
            ingest_email_message,
            content=item.content,
            subject=item.subject,
            client_id=item.client_id,
            message_id=item.message_id,
            sender=item.sender,
            received_at=item.received_at,
            media_urls=item.media_urls,
        )
        results.append(res)

    await manager.broadcast_to_all({"event": "inbox_updated", "refresh_tree": True})
    return {
        "processed": len(results),
        "created": sum(1 for r in results if r["status"] == "created"),
        "updated": sum(1 for r in results if r["status"] == "updated"),
        "duplicates": sum(1 for r in results if r["status"] == "duplicate"),
        "ignored": sum(1 for r in results if r["status"] == "ignored"),
        "results": results,
    }


@app.get("/api/inbox")
def api_list_inbox_messages(
    client_id: Optional[str] = Query(None, description="Optional client_id filter"),
    limit: int = Query(50, ge=1, le=200),
):
    """List received client email entries in the server Inbox along with their extraction statuses."""
    rows = get_inbox_messages(client_id=client_id, limit=limit)
    items = [r.to_dict() for r in rows]
    return {
        "count": len(items),
        "messages": items,
    }


@app.get("/api/events")
def api_list_unified_events(
    source_type: Optional[str] = Query(None, description="Filter by 'facebook' or 'email'"),
    client_id: Optional[str] = Query(None, description="Filter by client_id"),
    fanpage_id: Optional[int] = Query(None, description="Filter by fanpage_id"),
    limit: int = Query(100, ge=1, le=500),
):
    """Return deduplicated & updated events from the unified EventRecord table."""
    rows = get_all_events(source_type=source_type, client_id=client_id, fanpage_id=fanpage_id, limit=limit)
    items = [r.to_dict() for r in rows]
    return {
        "count": len(items),
        "events": items,
    }


@app.get("/api/fanpages")
def api_list_fanpages():
    rows = list_fanpages(active_only=False)
    return [
        {**fp.to_dict(), "post_count": count}
        for fp, count in rows
    ]


@app.get("/api/posts")
def api_list_all_posts(limit: int = Query(50, ge=1, le=200)):
    """Return latest scraped posts across all tracked Facebook fanpages."""
    posts = get_all_recent_posts(limit=limit)
    return {
        "count": len(posts),
        "posts": [p.to_dict() for p in posts],
    }


@app.post("/api/scrape")
async def api_run_scrape_now(target: Optional[str] = Query(None, description="Optional fanpage slug/ID/URL")):
    """Unified scrape trigger: runs locally on the VM by default, or dispatches GitHub Actions if --no-scrape."""
    use_actions_env = os.environ.get("USE_GITHUB_ACTIONS_CRAWLER", "").strip().lower() in ("1", "true", "yes")
    if not SERVER_AUTO_SCRAPE or use_actions_env:
        return await trigger_unified_scrape(target_slug_or_id=target)

    async with manager.scrape_lock:
        await manager.broadcast_to_all({"event": "status", "message": "Scraping fanpages for new posts..."})
        new_count = await run_scrape_cycle(target_slug_or_id=target, on_new_posts=manager.on_new_posts_scraped)
        await asyncio.to_thread(backup_state_to_git, "API VM crawl")
        await manager.broadcast_to_all(
            {
                "event": "status",
                "message": f"Scrape finished! Found {new_count} new post(s).",
                "refresh_tree": True,
            }
        )
    return {"status": "ok", "mode": "local", "new_posts_count": new_count}


async def process_single_post_url(url: str) -> Dict[str, Any]:
    """
    Shared handler for scraping a single Facebook post URL, feeding it to the Event Extractor,
    auto-adding its fanpage if it is an event, refreshing feeds, and backing up to Git.
    """
    clean_url = (url or "").strip()
    if not clean_url:
        raise HTTPException(status_code=400, detail="Post URL is required.")

    result = await extract_single_post_url(clean_url)
    if result.get("status") == "error":
        raise HTTPException(status_code=422, detail=result.get("detail") or "Failed to extract post.")

    if result.get("is_event"):
        ev_title = (result.get("event") or {}).get("title") or "Event"
        fp = result.get("fanpage")
        if result.get("fanpage_added") and fp:
            msg = f"Event found: '{ev_title}'. Added fanpage '{fp.get('name') or fp.get('slug')}' to tracked list."
        elif fp:
            msg = f"Event found: '{ev_title}' (fanpage '{fp.get('name') or fp.get('slug')}' already tracked)."
        else:
            msg = f"Event found: '{ev_title}' (fanpage link could not be resolved)."

        await manager.broadcast_to_all({"event": "status", "message": msg, "refresh_tree": True})
        if result.get("post") and fp:
            await manager.send_to_user(
                "__public__",
                {"event": "new_posts", "fanpage": fp, "posts": [result["post"]]},
            )
        asyncio.create_task(asyncio.to_thread(backup_state_to_git, "single post extract"))
    else:
        msg = result.get("detail") or "Post fetched and analyzed, but it is not an event (fanpage not added)."

    return {**result, "message": msg}


@app.post("/api/extract-post")
async def api_extract_single_post(body: ExtractPostRequest):
    """Fetch a single Facebook post URL, feed it to the extractor, and add its fanpage if it is an event."""
    return await process_single_post_url(body.url)


@app.get("/api/users/{client_id}/subscriptions")
def api_get_user_subscriptions(client_id: str):
    get_or_create_user(client_id)
    subs = get_user_subscriptions(client_id)
    return {
        "client_id": client_id,
        "rss_url": f"/users/{client_id}/rss.xml",
        "subscriptions": [fp.to_dict() for fp in subs],
    }


@app.post("/api/users/{client_id}/subscriptions")
async def api_subscribe_user(client_id: str, body: SubscribeRequest):
    try:
        subs, needs_scrape = subscribe_user_to_fanpages(client_id, body.urls)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    generate_user_rss_xml(client_id)

    # Notify connected WebSocket sessions for this user
    posts = get_posts_for_user(client_id, limit=50)
    await manager.send_to_user(
        client_id,
        {
            "event": "subscriptions_updated",
            "subscriptions": [fp.to_dict() for fp in subs],
            "posts": [p.to_dict() for p in posts],
        },
    )

    # If any subscribed fanpage has never been scraped, dispatch unified scrape (Actions or local)
    for fp in needs_scrape:
        await trigger_unified_scrape(fp.slug)

    return {
        "client_id": client_id,
        "subscriptions": [fp.to_dict() for fp in subs],
        "scraping_queued": [fp.url for fp in needs_scrape],
    }


@app.delete("/api/users/{client_id}/subscriptions")
async def api_unsubscribe_user(client_id: str, url: str = Query(..., description="Fanpage URL, slug, or ID")):
    removed = unsubscribe_user_from_fanpage(client_id, url)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Subscription '{url}' not found for user '{client_id}'.")

    subs = get_user_subscriptions(client_id)
    posts = get_posts_for_user(client_id, limit=50)
    generate_user_rss_xml(client_id)

    await manager.send_to_user(
        client_id,
        {
            "event": "subscriptions_updated",
            "subscriptions": [fp.to_dict() for fp in subs],
            "posts": [p.to_dict() for p in posts],
        },
    )
    return {"client_id": client_id, "removed": removed.to_dict(), "subscriptions": [fp.to_dict() for fp in subs]}


@app.get("/api/users/{client_id}/posts")
def api_get_user_posts(client_id: str, limit: int = Query(50, ge=1, le=200)):
    posts = get_posts_for_user(client_id, limit=limit)
    return {
        "client_id": client_id,
        "count": len(posts),
        "posts": [p.to_dict() for p in posts],
    }


@app.get("/users/{client_id}/rss.xml")
def api_get_user_rss(client_id: str):
    """Return personalized RSS 2.0 XML feed for `client_id`'s selected fanpages."""
    xml_content = generate_user_rss_xml(client_id)
    return Response(content=xml_content, media_type="application/rss+xml; charset=utf-8")


@app.get("/feeds")
@app.get("/feeds/")
def serve_feeds_root():
    """Return the JSON directory tree index of `/feeds/`."""
    return _build_feeds_directory_tree()


@app.get("/feeds/{file_path:path}")
def serve_feed_file(file_path: str):
    """
    Serve distributed `.json` (`events.json`, `messages.json`) and `.xml` files
    from `feeds/` and its subdirectories (`all/`, `fanpages/<slug>/`, `emails/<client_id>/`, `inbox/`).
    """
    base = FEEDS_DIR.resolve()
    target = (FEEDS_DIR / file_path).resolve()
    if base not in target.parents and target != base:
        raise HTTPException(status_code=403, detail="Access denied.")

    if target.is_dir():
        files = [
            _inspect_feed_file(p)
            for p in sorted(target.rglob("*"))
            if p.is_file() and p.suffix.lower() in (".json", ".xml")
        ]
        return JSONResponse(
            content={
                "folder": f"/feeds/{file_path.strip('/')}/",
                "count": len(files),
                "files": files,
            }
        )

    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Feed file '{file_path}' not found.")

    if target.suffix.lower() == ".json":
        return FileResponse(target, media_type="application/json; charset=utf-8")
    return FileResponse(target, media_type="application/rss+xml; charset=utf-8")


# --- WebSocket Endpoints ---


@app.websocket("/ws")
async def public_websocket_endpoint(websocket: WebSocket):
    """
    Shared real-time WebSocket endpoint for the main server page (no per-client separation).
    Sends all tracked fanpages and recent posts on connect, and supports {"action": "refresh"}.
    """
    await manager.connect("__public__", websocket)
    fanpages = [{**fp.to_dict(), "post_count": count} for fp, count in list_fanpages(active_only=False)]
    posts = get_all_recent_posts(limit=50)
    await websocket.send_json(
        {
            "event": "init",
            "fanpages": fanpages,
            "posts": [p.to_dict() for p in posts],
        }
    )

    try:
        while True:
            data = await websocket.receive_json()
            if data.get("action") == "refresh":
                res = await trigger_unified_scrape()
                if res.get("status") == "busy":
                    await websocket.send_json({"event": "status", "message": res["message"]})
    except WebSocketDisconnect:
        manager.disconnect("__public__", websocket)
    except Exception:
        manager.disconnect("__public__", websocket)


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """
    Real-time WebSocket endpoint for a specific user (`client_id`).
    Supported client JSON messages:
      - {"action": "subscribe", "urls": ["https://www.facebook.com/nasa"]}
      - {"action": "unsubscribe", "url": "https://www.facebook.com/nasa"}
      - {"action": "refresh"}
    """
    get_or_create_user(client_id)
    await manager.connect(client_id, websocket)

    # Send initial state (user's selected fanpages + recent posts from DB)
    subs = get_user_subscriptions(client_id)
    posts = get_posts_for_user(client_id, limit=50)
    await websocket.send_json(
        {
            "event": "init",
            "client_id": client_id,
            "rss_url": f"/users/{client_id}/rss.xml",
            "subscriptions": [fp.to_dict() for fp in subs],
            "posts": [p.to_dict() for p in posts],
        }
    )

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "subscribe":
                raw_urls = data.get("urls") or []
                if isinstance(raw_urls, str):
                    raw_urls = [raw_urls]
                try:
                    subs, needs_scrape = subscribe_user_to_fanpages(client_id, raw_urls)
                    generate_user_rss_xml(client_id)
                    posts = get_posts_for_user(client_id, limit=50)
                    await manager.send_to_user(
                        client_id,
                        {
                            "event": "subscriptions_updated",
                            "subscriptions": [fp.to_dict() for fp in subs],
                            "posts": [p.to_dict() for p in posts],
                        },
                    )
                    for fp in needs_scrape:
                        await trigger_unified_scrape(fp.slug)
                except ValueError as exc:
                    await websocket.send_json({"event": "error", "message": str(exc)})

            elif action == "unsubscribe":
                target_url = data.get("url", "")
                unsubscribe_user_from_fanpage(client_id, target_url)
                generate_user_rss_xml(client_id)
                subs = get_user_subscriptions(client_id)
                posts = get_posts_for_user(client_id, limit=50)
                await manager.send_to_user(
                    client_id,
                    {
                        "event": "subscriptions_updated",
                        "subscriptions": [fp.to_dict() for fp in subs],
                        "posts": [p.to_dict() for p in posts],
                    },
                )

            elif action == "refresh":
                res = await trigger_unified_scrape()
                if res.get("status") == "busy":
                    await websocket.send_json({"event": "status", "message": res["message"]})

    except WebSocketDisconnect:
        manager.disconnect(client_id, websocket)
    except Exception:
        manager.disconnect(client_id, websocket)


# --- Built-in Directory Tree + Client Email Inbox + Live Feed Dashboard ---


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Event Extractor Hub — Directory Tree & Client Email Inbox</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #f6f3ff;
      --card: #ffffff;
      --accent: #7c69ef;
      --accent-soft: #ece8ff;
      --mint: #d6f5e8;
      --mint-text: #1f7a58;
      --amber: #fff0d6;
      --amber-text: #9c6514;
      --rose: #ffdce6;
      --rose-text: #b83b5e;
      --sky: #dcecff;
      --sky-text: #2e62a6;
      --text: #2f2a44;
      --muted: #7e7896;
      --border: #e5dff7;
      --code-bg: #1f1d2e;
      --code-text: #e6e1ff;
    }
    * { box-sizing: border-box; }
    body {
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
      background: linear-gradient(135deg, #f6f3ff 0%, #eaf4ff 55%, #fff1f6 100%);
      background-attachment: fixed;
      color: var(--text);
      margin: 0;
      padding: 28px 36px;
      font-size: 15px;
    }
    .container { max-width: 1440px; margin: 0 auto; }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 20px;
      flex-wrap: wrap;
      background: var(--card);
      padding: 22px 28px;
      border-radius: 20px;
      margin-bottom: 24px;
      box-shadow: 0 6px 24px #b8a9e822;
    }
    .header h2 { margin: 0; font-size: 23px; }
    .sub-links { display: flex; gap: 14px; align-items: center; flex-wrap: wrap; margin-top: 6px; font-size: 13.5px; }
    .badge { padding: 6px 13px; border-radius: 999px; font-size: 12.5px; font-weight: 600; background: var(--mint); color: var(--mint-text); }
    .badge.sky { background: var(--sky); color: var(--sky-text); }
    .badge.amber { background: var(--amber); color: var(--amber-text); }
    .badge.rose { background: var(--rose); color: var(--rose-text); }
    .badge.muted { background: #efedf7; color: var(--muted); }
    .grid-2 { display: grid; grid-template-columns: 1.08fr 1fr; gap: 24px; align-items: start; margin-bottom: 24px; }
    .panel {
      background: var(--card);
      padding: 24px 26px;
      border-radius: 20px;
      margin-bottom: 24px;
      box-shadow: 0 6px 24px #b8a9e822;
    }
    .panel-title {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }
    .panel-title h3 { margin: 0; font-size: 19px; }
    input, textarea {
      background: #faf9ff;
      border: 2px solid var(--border);
      color: var(--text);
      padding: 10px 14px;
      border-radius: 11px;
      font-size: 14px;
      outline: none;
      font-family: inherit;
      width: 100%;
    }
    input:focus, textarea:focus { border-color: var(--accent); }
    textarea { min-height: 110px; resize: vertical; }
    button {
      background: var(--accent);
      color: #fff;
      border: none;
      padding: 10px 18px;
      border-radius: 11px;
      cursor: pointer;
      font-weight: 600;
      font-size: 14px;
      transition: transform .1s, filter .15s;
    }
    button:hover { filter: brightness(1.06); transform: translateY(-1px); }
    button.secondary { background: var(--sky); color: var(--sky-text); }
    button.ghost { background: var(--accent-soft); color: var(--accent); padding: 5px 11px; font-size: 12.5px; border-radius: 8px; }
    button.danger { background: var(--rose); color: var(--rose-text); padding: 4px 10px; font-size: 12.5px; margin-left: 8px; border-radius: 8px; }

    /* Directory Tree Styles */
    .tree-root {
      font-family: 'Cascadia Code', 'Consolas', monospace;
      background: #fbfaff;
      border: 1.5px solid var(--border);
      border-radius: 14px;
      padding: 16px 18px;
    }
    details.tree-folder {
      margin: 6px 0;
      border-left: 2px dashed #d7cff5;
      padding-left: 12px;
    }
    details.tree-folder > summary {
      cursor: pointer;
      font-weight: 700;
      color: #463a7c;
      padding: 6px 8px;
      border-radius: 8px;
      list-style: none;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    details.tree-folder > summary:hover { background: var(--accent-soft); }
    .tree-folder-desc { font-family: 'Segoe UI', system-ui, sans-serif; font-size: 12.5px; color: var(--muted); margin: 2px 0 8px 8px; }
    .tree-files { list-style: none; padding-left: 18px; margin: 4px 0 10px 0; }
    .tree-file {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      padding: 6px 10px;
      border-radius: 8px;
      font-size: 13.5px;
      border-bottom: 1px solid #f1eefb;
    }
    .tree-file:hover { background: #f3f0ff; }
    .tree-file-left { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; word-break: break-all; }
    .tree-file-actions { display: flex; align-items: center; gap: 6px; flex-shrink: 0; }

    /* Inbox Form & List */
    .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
    .inbox-list { max-height: 460px; overflow-y: auto; display: flex; flex-direction: column; gap: 12px; margin-top: 14px; }
    .inbox-item {
      border: 1.5px solid var(--border);
      border-radius: 14px;
      padding: 14px 16px;
      background: #fcfbff;
    }
    .inbox-meta { display: flex; justify-content: space-between; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 6px; font-size: 13px; color: var(--muted); }
    .changes-box {
      background: var(--amber);
      color: var(--amber-text);
      padding: 8px 12px;
      border-radius: 10px;
      font-size: 13px;
      margin-top: 8px;
    }
    pre.code-block {
      background: var(--code-bg);
      color: var(--code-text);
      padding: 14px 16px;
      border-radius: 12px;
      overflow-x: auto;
      font-size: 12.5px;
      line-height: 1.5;
      margin: 10px 0 0 0;
    }
    .chips { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; }
    .chip { background: var(--accent-soft); padding: 7px 14px; border-radius: 999px; display: flex; align-items: center; font-size: 13.5px; }
    #postsContainer { display: grid; grid-template-columns: repeat(auto-fill, minmax(400px, 1fr)); gap: 20px; }
    .post { background: var(--card); padding: 20px 22px; border-radius: 18px; border-top: 6px solid var(--accent); box-shadow: 0 6px 24px #b8a9e822; }
    .post-meta { display: flex; justify-content: space-between; gap: 10px; flex-wrap: wrap; color: var(--muted); font-size: 13.5px; margin-bottom: 10px; }
    .post-content { white-space: pre-wrap; line-height: 1.65; margin-bottom: 12px; word-break: break-word; }
    .post-imgs { display: flex; gap: 10px; overflow-x: auto; }
    .post-imgs img { max-height: 220px; border-radius: 10px; }
    a { color: #526fd4; text-decoration: none; font-weight: 500; }
    a:hover { text-decoration: underline; }
    #status { color: var(--muted); font-size: 13.5px; margin-top: 4px; }
    #jsonModal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(25, 20, 45, 0.55);
      z-index: 999;
      align-items: center;
      justify-content: center;
      padding: 24px;
    }
    .modal-card {
      background: var(--card);
      width: min(920px, 96vw);
      max-height: 86vh;
      border-radius: 20px;
      padding: 22px 26px;
      display: flex;
      flex-direction: column;
      box-shadow: 0 18px 50px rgba(0,0,0,0.28);
    }
    @media (max-width: 980px) {
      body { padding: 16px; }
      .grid-2 { grid-template-columns: 1fr; }
      .form-grid { grid-template-columns: 1fr; }
      #postsContainer { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="container">
    <!-- Top Header -->
    <div class="header">
      <div>
        <h2>Event Extractor Hub — Directory Tree &amp; Client Email Inbox</h2>
        <div id="status">Connecting real-time WebSocket...</div>
        <div class="sub-links">
          <span>🌐 Public Endpoint: <a href="https://stimulus-rectangle-ship.ngrok-free.dev/" target="_blank">https://stimulus-rectangle-ship.ngrok-free.dev/</a></span>
          <span>· <a href="/feeds/all/events.json" target="_blank">All Events JSON ↗</a></span>
          <span>· <a href="/feeds/emails/events.json" target="_blank">Email Events JSON ↗</a></span>
          <span>· <a href="/feeds/all.xml" target="_blank">Combined RSS XML ↗</a></span>
          <span>· <a href="/api/tree" target="_blank">API Tree Index ↗</a></span>
          <span>· <a href="/docs" target="_blank">OpenAPI Docs ↗</a></span>
        </div>
      </div>
      <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
        <span id="scheduleBadge" class="badge sky">Schedule: 08:00, 14:00, 20:00 ICT</span>
        <span id="wsBadge" class="badge">Disconnected</span>
        <button onclick="openAdminLogin()">🔐 Admin Login</button>
      </div>
    </div>

    <!-- Main 2-Column Grid: Directory Tree (Left) + Email Inbox (Right) -->
    <div class="grid-2">
      <!-- Column 1: Directory Tree of Distributed JSON & XML Feeds -->
      <div class="panel" style="margin-bottom:0;">
        <div class="panel-title">
          <div>
            <h3>📁 Distributed Feeds Directory Tree</h3>
            <div style="font-size:13px; color:var(--muted); margin-top:3px;">
              Extracted <code>events.json</code> &amp; <code>.xml</code> files organized by folder for easy client fetching
            </div>
          </div>
          <button class="secondary" onclick="loadDirectoryTree()">↻ Refresh Tree</button>
        </div>

        <div id="directoryTreeContainer" class="tree-root">
          Loading directory tree...
        </div>
      </div>

      <!-- Column 2: Client Email Inbox -->
      <div class="panel" style="margin-bottom:0;">
        <div class="panel-title">
          <div>
            <h3>📥 Client Email Inbox &rarr; Extractor</h3>
            <div style="font-size:13px; color:var(--muted); margin-top:3px;">
              Receive event-related emails from clients, compare anchor fields (Title, Format, Date, Time, Location), deduplicate or update, and extract JSON
            </div>
          </div>
          <span class="badge sky">POST /api/inbox</span>
        </div>

        <!-- Submit Email Form -->
        <div class="form-grid">
          <input id="emailClientId" placeholder="Client ID (e.g. client_team_1)" value="client_team_1" />
          <input id="emailSender" placeholder="Sender (e.g. clb-ai@fit.hcmus.edu.vn)" />
        </div>
        <div class="form-grid">
          <input id="emailSubject" placeholder="Email Subject (Event title / announcement)" />
          <input id="emailMessageId" placeholder="Message-ID (optional, auto-hashed if blank)" />
        </div>
        <div style="margin-bottom:10px;">
          <textarea id="emailContent" placeholder="Paste email content here... (e.g. Dear students, join our AI Workshop on 2026-11-20 at 14:00 at Hall I, HCMUS. Register at https://forms.gle/demo)"></textarea>
        </div>
        <div style="display:flex; gap:10px; align-items:center; margin-bottom:12px;">
          <input id="emailMediaUrls" placeholder="Optional Poster/QR Image URL(s), comma-separated" style="flex:1;" />
          <button onclick="submitInboxEmail()" id="submitEmailBtn">Send to Extractor &rarr;</button>
        </div>
        <div id="inboxSubmitMsg" style="font-size:13.5px; min-height:20px;"></div>

        <details style="margin-top:8px;">
          <summary style="cursor:pointer; font-size:13px; color:var(--accent); font-weight:600;">
            💻 Teammate Client API Snippet (POST to Server Inbox)
          </summary>
          <pre class="code-block">POST https://stimulus-rectangle-ship.ngrok-free.dev/api/inbox
Content-Type: application/json

{
  "client_id": "client_team_1",
  "message_id": "msg-2026-001@mail.gmail.com",
  "subject": "[WORKSHOP] Building Agentic AI Systems",
  "sender": "fit-events@hcmus.edu.vn",
  "received_at": "2026-11-09T15:30:00+07:00",
  "content": "Time: 09:00 20/11/2026\\nLocation: Hall I (Offline)\\nRegister: https://forms.gle/abc",
  "media_urls": []
}</pre>
        </details>

        <hr style="border:none; border-top:1.5px solid var(--border); margin:16px 0 12px 0;" />
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <strong>Recent Inbox Entries (<span id="inboxCount">0</span>)</strong>
          <button class="ghost" onclick="loadInboxMessages()">↻ Refresh Inbox</button>
        </div>
        <div id="inboxListContainer" class="inbox-list"></div>
      </div>
    </div>

    <!-- Tracked Facebook Fanpages (Managed via /admin) -->
    <div class="panel">
      <div class="panel-title">
        <div>
          <h3 style="margin:0;">📡 Tracked Facebook Fanpages (<span id="fanpageCount">0</span>)</h3>
          <div style="font-size:13px; color:var(--muted); margin-top:3px;">
            Unified crawler targets across the server. Paste a single post URL below to extract an event &amp; auto-add its fanpage.
          </div>
        </div>
        <div style="display:flex; gap:8px; flex-wrap:wrap;">
          <button class="secondary" onclick="triggerRefresh()">↻ Run Crawler &amp; Extractor Now</button>
          <button onclick="openAdminLogin()">⚙ Manage Fanpages (Admin)</button>
        </div>
      </div>
      <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-bottom:10px;">
        <input id="postUrlInput" placeholder="Paste a Facebook post URL (e.g. https://www.facebook.com/.../posts/...) to extract event &amp; auto-add fanpage" style="flex:1; min-width:260px;" onkeydown="if(event.key==='Enter') extractSinglePostUrl()" />
        <button id="extractPostBtn" onclick="extractSinglePostUrl()">🔎 Extract Post URL</button>
      </div>
      <div id="postExtractMsg" style="font-size:13.5px; min-height:20px; margin-bottom:8px;"></div>
      <div id="fanpagesContainer" class="chips"></div>
    </div>

    <h3>Latest Scraped Posts (<span id="postCount">0</span> posts)</h3>
    <div id="postsContainer"></div>
  </div>

  <!-- Admin Login Modal -->
  <div id="adminLoginModal" onclick="if(event.target.id==='adminLoginModal') closeAdminLogin()" style="display:none; position:fixed; inset:0; background:rgba(25,20,45,0.55); z-index:1000; align-items:center; justify-content:center; padding:24px;">
    <div class="modal-card" style="width:min(460px, 94vw);">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
        <strong style="font-size:17px;">🔐 Admin Login — Manage Facebook Fanpages</strong>
        <button class="ghost" onclick="closeAdminLogin()">✕ Close</button>
      </div>
      <div style="font-size:13.5px; color:var(--muted); margin-bottom:12px;">
        Enter your server <code>ADMIN_TOKEN</code> to unlock the <code>/admin</code> fanpage management subpage.
      </div>
      <div style="display:flex; gap:8px;">
        <input id="adminTokenInput" type="password" placeholder="Enter ADMIN_TOKEN" onkeydown="if(event.key==='Enter') submitAdminLogin()" />
        <button id="adminLoginBtn" onclick="submitAdminLogin()">Login &rarr;</button>
      </div>
      <div id="adminLoginMsg" style="margin-top:10px; font-size:13.5px; min-height:20px;"></div>
    </div>
  </div>

  <!-- JSON Preview Modal -->
  <div id="jsonModal" onclick="if(event.target.id==='jsonModal') closeJsonModal()">
    <div class="modal-card">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
        <strong id="jsonModalTitle" style="font-size:16px;">JSON Preview</strong>
        <button class="ghost" onclick="closeJsonModal()">✕ Close</button>
      </div>
      <pre id="jsonModalBody" class="code-block" style="flex:1; overflow:auto; margin:0;"></pre>
    </div>
  </div>

  <script>
    let ws = null;
    let currentPosts = [];

    const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

    async function loadDirectoryTree() {
      const container = document.getElementById('directoryTreeContainer');
      try {
        const res = await fetch('/api/tree');
        const data = await res.json();
        const foldersHtml = (data.folders || []).map((f, idx) => {
          const files = f.files || [];
          const filesHtml = files.length
            ? `<ul class="tree-files">` + files.map(file => {
                const icon = file.type === 'json' ? '📄' : '📰';
                const countBadge = file.count !== null && file.count !== undefined
                  ? `<span class="badge ${file.count > 0 ? '' : 'muted'}">${file.count} item(s)</span>`
                  : '';
                const kb = (file.size_bytes / 1024).toFixed(1) + ' KB';
                return `
                  <li class="tree-file">
                    <div class="tree-file-left">
                      <span>├─ ${icon}</span>
                      <a href="${esc(file.path)}" target="_blank">${esc(file.relative_path)}</a>
                      ${countBadge}
                      <span style="color:var(--muted); font-size:12px;">(${kb})</span>
                    </div>
                    <div class="tree-file-actions">
                      ${file.type === 'json' ? `<button class="ghost" onclick="previewJson('${esc(file.path)}')">Preview</button>` : ''}
                      <button class="ghost" onclick="copyFetchUrl('${esc(file.path)}', this)">Copy URL</button>
                    </div>
                  </li>
                `;
              }).join('') + `</ul>`
            : `<div style="padding:4px 0 8px 20px; color:var(--muted); font-size:12.5px;">(empty folder)</div>`;

          return `
            <details class="tree-folder" ${idx < 4 ? 'open' : ''}>
              <summary>
                <span>📂 ${esc(f.folder)}</span>
                <span class="badge sky">${files.length} file(s)</span>
              </summary>
              <div class="tree-folder-desc">${esc(f.description || '')}</div>
              ${filesHtml}
            </details>
          `;
        }).join('');

        container.innerHTML = `
          <div style="font-weight:700; margin-bottom:6px; color:#2f2a44;">
            🌐 /feeds/ <span style="font-weight:400; font-size:12px; color:var(--muted);">(Updated: ${new Date(data.updated_at).toLocaleTimeString()})</span>
          </div>
          ${foldersHtml}
        `;
      } catch (err) {
        container.innerHTML = `<div style="color:var(--rose-text);">Failed to load directory tree: ${esc(err.message)}</div>`;
      }
    }

    async function previewJson(path) {
      const modal = document.getElementById('jsonModal');
      const title = document.getElementById('jsonModalTitle');
      const body = document.getElementById('jsonModalBody');
      title.textContent = path;
      body.textContent = 'Loading...';
      modal.style.display = 'flex';
      try {
        const res = await fetch(path);
        const json = await res.json();
        body.textContent = JSON.stringify(json, null, 2);
      } catch (e) {
        body.textContent = 'Error loading JSON: ' + e.message;
      }
    }

    function closeJsonModal() {
      document.getElementById('jsonModal').style.display = 'none';
    }

    function copyFetchUrl(path, btn) {
      const fullUrl = window.location.origin + path;
      navigator.clipboard.writeText(fullUrl);
      const old = btn.textContent;
      btn.textContent = '✓ Copied';
      setTimeout(() => { btn.textContent = old; }, 1500);
    }

    function statusBadgeHtml(status) {
      if (status === 'created') return `<span class="badge">✨ NEW EVENT</span>`;
      if (status === 'updated') return `<span class="badge amber">🔄 UPDATED EVENT</span>`;
      if (status === 'duplicate') return `<span class="badge sky">📌 DUPLICATE</span>`;
      if (status === 'ignored') return `<span class="badge muted">⏭ NOT AN EVENT</span>`;
      return `<span class="badge muted">${esc(status)}</span>`;
    }

    async function loadInboxMessages() {
      const listEl = document.getElementById('inboxListContainer');
      try {
        const res = await fetch('/api/inbox?limit=30');
        const data = await res.json();
        const msgs = data.messages || [];
        document.getElementById('inboxCount').textContent = msgs.length;
        if (!msgs.length) {
          listEl.innerHTML = `<div style="color:var(--muted); font-size:13.5px; padding:8px 0;">No email entries received yet. Submit one above or POST to <code>/api/inbox</code>!</div>`;
          return;
        }
        listEl.innerHTML = msgs.map(m => {
          const changes = m.changes || {};
          const changeKeys = Object.keys(changes);
          const changesHtml = changeKeys.length
            ? `<div class="changes-box"><strong>Updated Anchor Fields:</strong> ` +
              changeKeys.map(k => `<code>${esc(k)}</code>: ${esc(changes[k].old || 'null')} &rarr; <strong>${esc(changes[k].new)}</strong>`).join(' · ') +
              `</div>`
            : '';
          const ev = m.event;
          const evSummaryHtml = ev
            ? `<div style="margin-top:8px; font-size:13px; background:#f4f1ff; padding:8px 12px; border-radius:9px;">
                 <div><strong>Event:</strong> ${esc(ev.title)}</div>
                 <div style="color:var(--muted); margin-top:2px;">
                   📅 Date: <strong>${esc(ev.date || ev.start_datetime || 'TBA')}</strong> ·
                   ⏰ Time: <strong>${esc(ev.time || 'TBA')}</strong> ·
                   📍 Location: <strong>${esc(ev.location || 'TBA')}</strong> ·
                   🌐 Mode: <strong>${esc(ev.online_mode || (ev.is_online ? 'online' : 'offline'))}</strong>
                 </div>
               </div>`
            : '';
          return `
            <div class="inbox-item">
              <div class="inbox-meta">
                <span><strong>${esc(m.client_id)}</strong> · ${esc(m.sender || 'no-sender')} · <code style="font-size:11.5px;">${esc(m.message_id)}</code></span>
                ${statusBadgeHtml(m.status)}
              </div>
              <div style="font-weight:600; margin-bottom:4px;">${esc(m.subject || '(No Subject)')}</div>
              <div style="font-size:13px; color:#4b4468; white-space:pre-wrap;">${esc((m.content || '').slice(0, 220))}${(m.content || '').length > 220 ? '...' : ''}</div>
              ${changesHtml}
              ${evSummaryHtml}
            </div>
          `;
        }).join('');
      } catch (err) {
        listEl.innerHTML = `<div style="color:var(--rose-text);">Error loading inbox: ${esc(err.message)}</div>`;
      }
    }

    async function submitInboxEmail() {
      const btn = document.getElementById('submitEmailBtn');
      const msgEl = document.getElementById('inboxSubmitMsg');
      const client_id = document.getElementById('emailClientId').value.trim() || 'anonymous';
      const sender = document.getElementById('emailSender').value.trim() || null;
      const subject = document.getElementById('emailSubject').value.trim();
      const message_id = document.getElementById('emailMessageId').value.trim() || null;
      const content = document.getElementById('emailContent').value.trim();
      const rawMedia = document.getElementById('emailMediaUrls').value.trim();
      const media_urls = rawMedia ? rawMedia.split(',').map(s => s.trim()).filter(Boolean) : [];

      if (!content) {
        msgEl.style.color = '#b83b5e';
        msgEl.textContent = 'Please enter email content to extract.';
        return;
      }

      btn.disabled = true;
      msgEl.style.color = '#7e7896';
      msgEl.textContent = 'Running unified extractor & anchor-field comparison...';

      try {
        const res = await fetch('/api/inbox', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ client_id, message_id, subject, sender, content, media_urls }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || res.statusText);

        msgEl.style.color = '#1f7a58';
        msgEl.innerHTML = `Result: ${statusBadgeHtml(data.status)} ${data.event ? '— <strong>' + esc(data.event.title) + '</strong>' : ''}`;
        document.getElementById('emailMessageId').value = '';
        await loadInboxMessages();
        await loadDirectoryTree();
      } catch (err) {
        msgEl.style.color = '#b83b5e';
        msgEl.textContent = 'Error: ' + err.message;
      } finally {
        btn.disabled = false;
      }
    }

    async function openAdminLogin() {
      const saved = sessionStorage.getItem('adminToken');
      if (saved) {
        try {
          const res = await fetch('/api/admin/verify', { headers: { 'X-Admin-Token': saved } });
          if (res.ok) {
            window.location.href = '/admin';
            return;
          }
        } catch (e) {}
      }
      document.getElementById('adminLoginMsg').textContent = '';
      document.getElementById('adminTokenInput').value = '';
      document.getElementById('adminLoginModal').style.display = 'flex';
      setTimeout(() => document.getElementById('adminTokenInput').focus(), 50);
    }

    function closeAdminLogin() {
      document.getElementById('adminLoginModal').style.display = 'none';
    }

    async function submitAdminLogin() {
      const token = document.getElementById('adminTokenInput').value.trim();
      const msgEl = document.getElementById('adminLoginMsg');
      const btn = document.getElementById('adminLoginBtn');
      if (!token) {
        msgEl.style.color = '#b83b5e';
        msgEl.textContent = 'Please enter your ADMIN_TOKEN.';
        return;
      }
      btn.disabled = true;
      msgEl.style.color = '#7e7896';
      msgEl.textContent = 'Verifying admin token...';
      try {
        const res = await fetch('/api/admin/verify', { headers: { 'X-Admin-Token': token } });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || res.statusText);
        sessionStorage.setItem('adminToken', token);
        window.location.href = '/admin';
      } catch (err) {
        msgEl.style.color = '#b83b5e';
        msgEl.textContent = err.message;
      } finally {
        btn.disabled = false;
      }
    }

    async function loadScheduleBadge() {
      try {
        const res = await fetch('/api/schedule');
        const data = await res.json();
        const badge = document.getElementById('scheduleBadge');
        if (data.mode === 'daily_cron') {
          badge.textContent = 'Daily: ' + (data.scheduled_times_ict || []).join(', ') + ' ICT';
        } else {
          badge.textContent = 'Interval: ' + data.interval_seconds + 's';
        }
      } catch (e) {}
    }

    function connectWS() {
      if (ws) ws.close();
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(`${proto}//${location.host}/ws`);

      ws.onopen = () => {
        document.getElementById('wsBadge').textContent = 'Live Connected';
        document.getElementById('status').textContent = 'Listening for real-time crawler & client inbox events...';
      };

      ws.onmessage = (evt) => {
        const msg = JSON.parse(evt.data);
        if (msg.event === 'init') {
          renderFanpages(msg.fanpages || []);
          currentPosts = msg.posts || [];
          renderPosts();
        } else if (msg.event === 'new_posts') {
          document.getElementById('status').textContent = `New post(s) arrived from ${msg.fanpage.name}!`;
          currentPosts = [...msg.posts, ...currentPosts];
          renderPosts();
          loadFanpages();
          loadDirectoryTree();
        } else if (msg.event === 'inbox_updated') {
          loadInboxMessages();
          loadDirectoryTree();
        } else if (msg.event === 'status' || msg.event === 'error') {
          document.getElementById('status').textContent = msg.message;
          if (msg.refresh_tree) {
            loadDirectoryTree();
            loadFanpages();
          }
        }
      };

      ws.onclose = () => {
        document.getElementById('wsBadge').textContent = 'Reconnecting...';
        setTimeout(connectWS, 4000);
      };
    }

    async function loadFanpages() {
      try {
        const res = await fetch('/api/fanpages');
        const list = await res.json();
        renderFanpages(list || []);
      } catch (e) {}
    }

    function triggerRefresh() {
      if (!ws) return;
      ws.send(JSON.stringify({ action: 'refresh' }));
    }

    async function extractSinglePostUrl() {
      const input = document.getElementById('postUrlInput');
      const msgEl = document.getElementById('postExtractMsg');
      const btn = document.getElementById('extractPostBtn');
      const url = (input.value || '').trim();
      if (!url) {
        msgEl.style.color = 'var(--rose-text)';
        msgEl.textContent = 'Please paste a Facebook post URL.';
        return;
      }
      btn.disabled = true;
      btn.textContent = 'Extracting...';
      msgEl.style.color = 'var(--muted)';
      msgEl.textContent = 'Fetching Facebook post & running Event Extractor...';
      try {
        const res = await fetch('/api/extract-post', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || res.statusText);
        msgEl.style.color = data.is_event ? 'var(--mint-text)' : 'var(--muted)';
        msgEl.textContent = data.message || (data.is_event ? 'Event extracted!' : 'Not an event.');
        if (data.is_event) {
          input.value = '';
          loadDirectoryTree();
          loadFanpages();
        }
      } catch (err) {
        msgEl.style.color = 'var(--rose-text)';
        msgEl.textContent = 'Error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.textContent = '🔎 Extract Post URL';
      }
    }

    function renderFanpages(list) {
      document.getElementById('fanpageCount').textContent = list.length;
      const el = document.getElementById('fanpagesContainer');
      if (!list.length) {
        el.innerHTML = '<span style="color:#8c86a3;font-size:14px;">No Facebook fanpages tracked yet. Log into Admin to add fanpages!</span>';
        return;
      }
      el.innerHTML = list.map(f => `
        <div class="chip">
          <span>
            <strong>${esc(f.name)}</strong>
            (${f.post_count ?? 0} posts ·
            <a href="/feeds/fanpages/${encodeURIComponent(f.slug)}/events.json" target="_blank">events.json</a> ·
            <a href="/feeds/${encodeURIComponent(f.slug)}.xml" target="_blank">rss.xml</a>)
          </span>
        </div>
      `).join('');
    }

    function renderPosts() {
      document.getElementById('postCount').textContent = currentPosts.length;
      const el = document.getElementById('postsContainer');
      el.innerHTML = currentPosts.map(p => `
        <div class="post">
          <div class="post-meta">
            <strong>${esc(p.fanpage_name || 'Fanpage')}</strong>
            <span>${p.published_at ? new Date(p.published_at).toLocaleString() : ''} · <a href="${esc(p.post_url)}" target="_blank">View on FB ↗</a></span>
          </div>
          <div class="post-content">${esc(p.content || '')}</div>
          <div class="post-imgs">
            ${(p.media_urls || []).map(u => `<img src="${esc(u)}" loading="lazy" />`).join('')}
          </div>
        </div>
      `).join('');
    }

    loadScheduleBadge();
    loadDirectoryTree();
    loadInboxMessages();
    connectWS();
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)
