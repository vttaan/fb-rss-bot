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


# --- REST API Endpoints ---


@app.get("/api/fanpages")
def api_list_fanpages():
    rows = list_fanpages(active_only=False)
    return [
        {**fp.to_dict(), "post_count": count}
        for fp, count in rows
    ]


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
  <style>
    :root { --bg: #0f172a; --card: #1e293b; --accent: #3b82f6; --text: #f8fafc; --muted: #94a3b8; }
    body { font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 24px; }
    .container { max-width: 960px; margin: 0 auto; }
    .header { display: flex; justify-content: space-between; align-items: center; background: var(--card); padding: 16px 20px; border-radius: 12px; margin-bottom: 20px; }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; background: #22c55e33; color: #4ade80; }
    .panel { background: var(--card); padding: 18px 20px; border-radius: 12px; margin-bottom: 20px; }
    input { background: #0f172a; border: 1px solid #334155; color: var(--text); padding: 10px 14px; border-radius: 8px; width: 320px; }
    button { background: var(--accent); color: white; border: none; padding: 10px 16px; border-radius: 8px; cursor: pointer; font-weight: 600; }
    button.secondary { background: #334155; }
    button.danger { background: #ef4444; padding: 4px 10px; font-size: 12px; margin-left: 8px; }
    .chips { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .chip { background: #0f172a; border: 1px solid #334155; padding: 6px 12px; border-radius: 8px; display: flex; align-items: center; font-size: 14px; }
    .post { background: var(--card); padding: 18px 20px; border-radius: 12px; margin-bottom: 14px; border-left: 4px solid var(--accent); }
    .post-meta { display: flex; justify-content: space-between; color: var(--muted); font-size: 13px; margin-bottom: 8px; }
    .post-content { white-space: pre-wrap; line-height: 1.5; margin-bottom: 12px; }
    .post-imgs { display: flex; gap: 10px; overflow-x: auto; }
    .post-imgs img { max-height: 220px; border-radius: 8px; }
    a { color: #60a5fa; text-decoration: none; }
    #status { color: #fbbf24; font-size: 13px; margin-top: 8px; }
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
        <a id="rssLink" href="/users/user_1/rss.xml" target="_blank">Personal RSS Feed (.xml) ↗</a>
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
        el.innerHTML = '<span style="color:#94a3b8;font-size:14px;">No fanpages selected yet. Add one above!</span>';
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
