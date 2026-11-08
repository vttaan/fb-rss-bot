import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel

from config import DEFAULT_INTERVAL_SECONDS, FEEDS_DIR
from database import (
    Fanpage,
    Post,
    get_or_create_user,
    get_posts_for_user,
    get_subscribers_for_fanpage,
    get_user_subscriptions,
    init_db,
    list_fanpages,
    subscribe_user_to_fanpages,
    unsubscribe_user_from_fanpage,
)
from rss import generate_user_rss_xml
from scheduler import run_scrape_cycle


class SubscribeRequest(BaseModel):
    urls: List[str]


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
        Pushes new posts ONLY to connected users subscribed to `fanpage.id`.
        """
        subscriber_ids = get_subscribers_for_fanpage(fanpage.id)
        if not subscriber_ids:
            return

        payload = {
            "event": "new_posts",
            "fanpage": fanpage.to_dict(),
            "posts": [p.to_dict() for p in new_posts],
        }
        for client_id in subscriber_ids:
            if client_id in self.active_connections:
                generate_user_rss_xml(client_id)
                await self.send_to_user(client_id, payload)


manager = ConnectionManager()
scheduler = AsyncIOScheduler()
SERVER_INTERVAL_SECONDS = DEFAULT_INTERVAL_SECONDS


async def scheduled_scrape_task() -> None:
    if manager.scrape_lock.locked():
        return
    async with manager.scrape_lock:
        await manager.broadcast_to_all({"event": "status", "message": "Running scheduled 1-hour fanpage scrape..."})
        new_count = await run_scrape_cycle(on_new_posts=manager.on_new_posts_scraped)
        await manager.broadcast_to_all(
            {"event": "status", "message": f"Scrape cycle finished ({new_count} new post(s) discovered)."}
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
        await manager.broadcast_to_all(
            {"event": "status", "message": f"Scrape finished! Found {new_count} new post(s)."}
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(
        scheduled_scrape_task,
        trigger="interval",
        seconds=SERVER_INTERVAL_SECONDS,
        id="server_hourly_scrape",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(
    title="Facebook Fanpage RSS & WebSocket Push Server",
    description="Scrapes public Facebook fanpages every 1 hour and streams new posts to subscribed users over WebSocket & RSS XML.",
    version="1.0.0",
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


# --- REST API Endpoints ---


@app.get("/api/fanpages")
def api_list_fanpages():
    rows = list_fanpages(active_only=False)
    return [
        {**fp.to_dict(), "post_count": count}
        for fp, count in rows
    ]


@app.post("/api/scrape")
async def api_run_scrape_now():
    """Run a full scrape cycle inside the server and broadcast new posts to WebSocket subscribers."""
    async with manager.scrape_lock:
        await manager.broadcast_to_all({"event": "status", "message": "Scraping fanpages for new posts..."})
        new_count = await run_scrape_cycle(on_new_posts=manager.on_new_posts_scraped)
        await manager.broadcast_to_all(
            {"event": "status", "message": f"Scrape finished! Found {new_count} new post(s)."}
        )
    return {"status": "ok", "new_posts_count": new_count}


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

    # If any subscribed fanpage has never been scraped, scrape it in background immediately
    for fp in needs_scrape:
        asyncio.create_task(trigger_background_scrape(fp.slug))

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


@app.get("/feeds/{filename}")
def serve_feed_file(filename: str):
    """Serve static .xml feed files from `feeds/` directory."""
    safe_name = Path(filename).name
    file_path = FEEDS_DIR / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Feed XML file not found.")
    return FileResponse(file_path, media_type="application/rss+xml; charset=utf-8")


# --- WebSocket Endpoint ---


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
                        asyncio.create_task(trigger_background_scrape(fp.slug))
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
                if not manager.scrape_lock.locked():
                    asyncio.create_task(trigger_background_scrape())
                else:
                    await websocket.send_json(
                        {"event": "status", "message": "A scrape cycle is already in progress..."}
                    )

    except WebSocketDisconnect:
        manager.disconnect(client_id, websocket)
    except Exception:
        manager.disconnect(client_id, websocket)


# --- Built-in Interactive Web Dashboard for Testing ---


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Facebook Fanpage RSS & WebSocket Live Feed</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root { --bg: #f6f3ff; --card: #ffffff; --accent: #9b8cf0; --accent-soft: #ece8ff; --mint: #d6f5e8; --mint-text: #2f8f6b; --peach: #ffe8dc; --rose: #ffdce6; --sky: #dcecff; --text: #3b3552; --muted: #8c86a3; --border: #e6e0f7; }
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: linear-gradient(135deg, #f6f3ff 0%, #eaf4ff 55%, #fff1f6 100%); background-attachment: fixed; color: var(--text); margin: 0; padding: 32px 40px; font-size: 16px; }
    .container { max-width: 1400px; margin: 0 auto; }
    .header { display: flex; justify-content: space-between; align-items: center; gap: 20px; flex-wrap: wrap; background: var(--card); padding: 22px 28px; border-radius: 20px; margin-bottom: 24px; box-shadow: 0 6px 24px #b8a9e822; }
    .header h2 { font-size: 24px; }
    .badge { padding: 6px 14px; border-radius: 999px; font-size: 13px; font-weight: 600; background: var(--mint); color: var(--mint-text); }
    .panel { background: var(--card); padding: 24px 28px; border-radius: 20px; margin-bottom: 24px; box-shadow: 0 6px 24px #b8a9e822; }
    input { background: #faf9ff; border: 2px solid var(--border); color: var(--text); padding: 12px 16px; border-radius: 12px; width: 320px; font-size: 15px; outline: none; }
    input:focus { border-color: var(--accent); }
    button { background: var(--accent); color: #fff; border: none; padding: 12px 20px; border-radius: 12px; cursor: pointer; font-weight: 600; font-size: 15px; transition: transform .1s, filter .15s; }
    button:hover { filter: brightness(1.06); transform: translateY(-1px); }
    button.secondary { background: var(--sky); color: #3d6fb5; }
    button.danger { background: var(--rose); color: #c2476b; padding: 4px 10px; font-size: 13px; margin-left: 10px; border-radius: 8px; }
    .chips { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 16px; }
    .chip { background: var(--accent-soft); padding: 8px 14px; border-radius: 999px; display: flex; align-items: center; font-size: 14px; }
    #postsContainer { display: grid; grid-template-columns: repeat(auto-fill, minmax(420px, 1fr)); gap: 22px; }
    .post { background: var(--card); padding: 22px 24px; border-radius: 20px; border-top: 6px solid var(--accent); box-shadow: 0 6px 24px #b8a9e822; }
    .post:nth-child(4n+2) { border-top-color: #8fd3b6; }
    .post:nth-child(4n+3) { border-top-color: #ffb89a; }
    .post:nth-child(4n+4) { border-top-color: #8fbdf2; }
    .post-meta { display: flex; justify-content: space-between; gap: 10px; flex-wrap: wrap; color: var(--muted); font-size: 14px; margin-bottom: 12px; }
    .post-content { white-space: pre-wrap; line-height: 1.7; margin-bottom: 14px; word-break: break-word; }
    .post-imgs { display: flex; gap: 10px; overflow-x: auto; }
    .post-imgs img { max-height: 260px; border-radius: 12px; }
    h3 { font-size: 20px; }
    a { color: #5b7fd6; text-decoration: none; }
    a:hover { text-decoration: underline; }
    #status { color: var(--muted); font-size: 14px; margin-top: 8px; }
    @media (max-width: 700px) { body { padding: 16px; } #postsContainer { grid-template-columns: 1fr; } input { width: 100%; } }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h2 style="margin:0;">Facebook Fanpage WebSocket + RSS Server</h2>
        <div id="status">Connecting WebSocket...</div>
      </div>
      <div>
        <label>User ID: </label>
        <input id="userInput" value="user_1" style="width:120px;" />
        <button onclick="connectWS()">Switch User</button>
        <span id="wsBadge" class="badge">Disconnected</span>
      </div>
    </div>

    <div class="panel">
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <h3 style="margin:0;">Your Selected Fanpages</h3>
        <span><a href="/admin" style="margin-right:16px;">Admin ⚙</a><a id="rssLink" href="/users/user_1/rss.xml" target="_blank">Personal RSS Feed (.xml) ↗</a></span>
      </div>
      <div style="margin-top:12px; display:flex; gap:8px;">
        <input id="fanpageInput" placeholder="https://www.facebook.com/nasa or TechCrunch" style="flex:1;" />
        <button onclick="subscribeFanpage()">+ Subscribe Fanpage</button>
        <button class="secondary" onclick="triggerRefresh()">Scrape Now</button>
      </div>
      <div id="subsContainer" class="chips"></div>
    </div>

    <h3>Live Feed (<span id="postCount">0</span> posts)</h3>
    <div id="postsContainer"></div>
  </div>

  <script>
    let ws = null;
    let currentPosts = [];

    function connectWS() {
      const userId = document.getElementById('userInput').value.trim() || 'user_1';
      document.getElementById('rssLink').href = `/users/${encodeURIComponent(userId)}/rss.xml`;
      if (ws) ws.close();

      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(`${proto}//${location.host}/ws/${encodeURIComponent(userId)}`);

      ws.onopen = () => {
        document.getElementById('wsBadge').textContent = 'Connected (' + userId + ')';
        document.getElementById('status').textContent = 'Listening for real-time fanpage updates (1-hour auto cycle)...';
      };

      ws.onmessage = (evt) => {
        const msg = JSON.parse(evt.data);
        if (msg.event === 'init' || msg.event === 'subscriptions_updated') {
          renderSubs(msg.subscriptions || []);
          currentPosts = msg.posts || [];
          renderPosts();
        } else if (msg.event === 'new_posts') {
          document.getElementById('status').textContent = `New post(s) arrived from ${msg.fanpage.name}!`;
          currentPosts = [...msg.posts, ...currentPosts];
          renderPosts();
        } else if (msg.event === 'status' || msg.event === 'error') {
          document.getElementById('status').textContent = msg.message;
        }
      };

      ws.onclose = () => {
        document.getElementById('wsBadge').textContent = 'Reconnecting...';
        setTimeout(connectWS, 4000);
      };
    }

    function subscribeFanpage() {
      const val = document.getElementById('fanpageInput').value.trim();
      if (!val || !ws) return;
      ws.send(JSON.stringify({ action: 'subscribe', urls: [val] }));
      document.getElementById('fanpageInput').value = '';
    }

    function unsubscribeFanpage(url) {
      if (!ws) return;
      ws.send(JSON.stringify({ action: 'unsubscribe', url }));
    }

    function triggerRefresh() {
      if (!ws) return;
      ws.send(JSON.stringify({ action: 'refresh' }));
    }

    function renderSubs(subs) {
      const el = document.getElementById('subsContainer');
      if (!subs.length) {
        el.innerHTML = '<span style="color:#8c86a3;font-size:14px;">No fanpages selected yet. Add one above!</span>';
        return;
      }
      el.innerHTML = subs.map(s => `
        <div class="chip">
          <span><strong>${s.name}</strong> (${s.url})</span>
          <button class="danger" onclick="unsubscribeFanpage('${s.url}')">✕</button>
        </div>
      `).join('');
    }

    function renderPosts() {
      document.getElementById('postCount').textContent = currentPosts.length;
      const el = document.getElementById('postsContainer');
      el.innerHTML = currentPosts.map(p => `
        <div class="post">
          <div class="post-meta">
            <strong>${p.fanpage_name || 'Fanpage'}</strong>
            <span>${p.published_at ? new Date(p.published_at).toLocaleString() : ''} · <a href="${p.post_url}" target="_blank">View on FB ↗</a></span>
          </div>
          <div class="post-content">${(p.content || '').replace(/</g, '&lt;')}</div>
          <div class="post-imgs">
            ${(p.media_urls || []).map(u => `<img src="${u}" loading="lazy" />`).join('')}
          </div>
        </div>
      `).join('');
    }

    connectWS();
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)
