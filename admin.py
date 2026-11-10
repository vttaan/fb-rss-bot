"""Admin sub-page (`/admin`) + token-protected API to add/remove tracked fanpages.

Protection: set env `ADMIN_TOKEN`. If it is unset, all admin API calls are disabled (503).
Clients send the token in the `X-Admin-Token` header.
"""
import asyncio
import hmac
import os
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from config import FEEDS_DIR
from database import add_fanpage, ensure_local_tz, list_fanpages, remove_fanpage, save_posts, update_fanpage_metadata
from events import analyze_fanpage_posts, purge_fanpage_events, sync_all_event_folders
from rss import generate_all_rss, generate_fanpage_rss

router = APIRouter()


class AddFanpageRequest(BaseModel):
    url: str
    name: str | None = None


class CrawledPostItem(BaseModel):
    post_id: str
    post_url: str = ""
    content: str = ""
    media_urls: List[str] = Field(default_factory=list)
    published_at: Optional[str] = None


class CrawledFanpagePayload(BaseModel):
    url: str
    name: Optional[str] = None
    posts: List[CrawledPostItem] = Field(default_factory=list)


class ImportCrawlRequest(BaseModel):
    fanpages: List[CrawledFanpagePayload] = Field(default_factory=list)


def require_admin(x_admin_token: str = Header(default="")) -> None:
    expected = os.environ.get("ADMIN_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=503, detail="Admin is disabled: ADMIN_TOKEN is not configured on the server.")
    if not hmac.compare_digest(x_admin_token.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid admin token.")


def _event_count(slug: str) -> int:
    import json

    path = FEEDS_DIR / slug / "events.json"
    try:
        return int(json.loads(path.read_text(encoding="utf-8")).get("count", 0))
    except Exception:
        return 0


@router.get("/api/admin/verify", dependencies=[Depends(require_admin)])
def admin_verify_token():
    """Verify X-Admin-Token from main-page login before redirecting to /admin."""
    return {"ok": True}


@router.post("/api/admin/scrape", dependencies=[Depends(require_admin)])
async def admin_trigger_scrape():
    """Trigger a full scrape + extractor cycle across all tracked Facebook fanpages."""
    from server import trigger_background_scrape

    asyncio.create_task(trigger_background_scrape())
    return {"status": "started"}


@router.post("/api/admin/import-crawl", dependencies=[Depends(require_admin)])
async def admin_import_crawl(body: ImportCrawlRequest):
    """
    Receive scraped fanpage posts pushed from the 3x/day GitHub Actions crawler,
    save new posts, run the unified event extractor, update XML/JSON folders, and
    notify connected dashboard WebSockets.
    """
    from server import manager

    total_new_posts = 0
    total_events = 0
    fanpages_synced = 0

    for fp_payload in body.fanpages:
        try:
            fp, _ = add_fanpage(fp_payload.url, fp_payload.name)
        except ValueError:
            continue

        update_fanpage_metadata(fp.id, name=fp_payload.name)
        if fp_payload.name:
            fp.name = fp_payload.name

        scraped_items = []
        for p in fp_payload.posts:
            pub_dt = None
            if p.published_at:
                try:
                    pub_dt = ensure_local_tz(datetime.fromisoformat(p.published_at.replace("Z", "+00:00")))
                except Exception:
                    pub_dt = None
            scraped_items.append(
                {
                    "post_id": p.post_id,
                    "post_url": p.post_url,
                    "content": p.content,
                    "media_urls": p.media_urls,
                    "published_at": pub_dt,
                }
            )

        new_posts = save_posts(fp.id, scraped_items)
        total_new_posts += len(new_posts)
        generate_fanpage_rss(fp)

        ev_stats = await asyncio.to_thread(analyze_fanpage_posts, fp.id)
        total_events += int(ev_stats.get("events", 0))
        fanpages_synced += 1

        if new_posts:
            await manager.on_new_posts_scraped(fp, new_posts)

    generate_all_rss()
    await asyncio.to_thread(sync_all_event_folders)
    await manager.broadcast_to_all(
        {
            "event": "status",
            "message": f"Synced scheduled crawl: {fanpages_synced} fanpage(s), {total_new_posts} new post(s).",
            "refresh_tree": True,
        }
    )
    return {
        "status": "ok",
        "fanpages_synced": fanpages_synced,
        "new_posts": total_new_posts,
        "events_found": total_events,
    }


@router.get("/api/admin/fanpages", dependencies=[Depends(require_admin)])
def admin_list_fanpages():
    return [
        {**fp.to_dict(), "post_count": count, "event_count": _event_count(fp.slug)}
        for fp, count in list_fanpages(active_only=False)
    ]


@router.post("/api/admin/fanpages", dependencies=[Depends(require_admin)])
async def admin_add_fanpage(body: AddFanpageRequest):
    from server import trigger_background_scrape

    try:
        fp, created = add_fanpage(body.url, body.name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if created:
        asyncio.create_task(trigger_background_scrape(fp.slug))
    return {"created": created, "fanpage": fp.to_dict()}


@router.delete("/api/admin/fanpages", dependencies=[Depends(require_admin)])
def admin_remove_fanpage(identifier: str = Query(..., description="Fanpage id, slug or URL")):
    removed = remove_fanpage(identifier)
    if not removed:
        raise HTTPException(status_code=404, detail="Fanpage not found.")
    (FEEDS_DIR / f"{removed.slug}.xml").unlink(missing_ok=True)
    purge_fanpage_events(removed.id, removed.slug)
    generate_all_rss()
    return {"removed": removed.to_dict()}


ADMIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Admin - Manage Fanpages</title>
  <style>
    :root { --card:#fff; --accent:#9b8cf0; --soft:#ece8ff; --mint:#d6f5e8; --rose:#ffdce6; --sky:#dcecff; --text:#3b3552; --muted:#8c86a3; --border:#e6e0f7; }
    * { box-sizing: border-box; }
    body { font-family:'Segoe UI',system-ui,sans-serif; background:linear-gradient(135deg,#f6f3ff 0%,#eaf4ff 55%,#fff1f6 100%); background-attachment:fixed; color:var(--text); margin:0; padding:32px 40px; }
    .container { max-width:1200px; margin:0 auto; }
    .panel { background:var(--card); padding:24px 28px; border-radius:20px; margin-bottom:24px; box-shadow:0 6px 24px #b8a9e822; }
    .row { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
    input { background:#faf9ff; border:2px solid var(--border); color:var(--text); padding:12px 16px; border-radius:12px; font-size:15px; outline:none; flex:1; min-width:220px; }
    input:focus { border-color:var(--accent); }
    button { background:var(--accent); color:#fff; border:none; padding:12px 20px; border-radius:12px; cursor:pointer; font-weight:600; font-size:15px; }
    button.secondary { background:var(--sky); color:#3d6fb5; }
    button.danger { background:var(--rose); color:#c2476b; padding:8px 14px; border-radius:10px; }
    table { width:100%; border-collapse:collapse; }
    th, td { text-align:left; padding:12px 10px; border-bottom:1px solid var(--border); font-size:15px; }
    th { color:var(--muted); font-weight:600; font-size:13px; text-transform:uppercase; }
    a { color:#5b7fd6; text-decoration:none; }
    #msg { margin-top:12px; font-size:14px; color:var(--muted); min-height:20px; }
    .pill { background:var(--mint); color:#2f8f6b; border-radius:999px; padding:3px 12px; font-size:13px; font-weight:600; }
  </style>
</head>
<body>
  <div class="container">
    <div class="panel row" style="justify-content:space-between;">
      <div>
        <h2 style="margin:0;">Admin: Manage All Facebook Fanpages</h2>
        <div style="font-size:13.5px; color:var(--muted); margin-top:4px;">Add, remove, or scrape tracked Facebook fanpages (feeds automatically sync to Directory Tree)</div>
      </div>
      <div class="row">
        <button class="secondary" id="scrapeAllBtn" onclick="scrapeAll()" style="display:none;">↻ Scrape All Now</button>
        <button class="danger" id="logoutBtn" onclick="logoutAdmin()" style="display:none;">🔒 Lock &amp; Logout</button>
        <a href="/">&larr; Back to main page</a>
      </div>
    </div>

    <div class="panel" id="unlockPanel">
      <div class="row">
        <input id="token" type="password" placeholder="Enter Admin Token (ADMIN_TOKEN)" onkeydown="if(event.key==='Enter')saveToken()" />
        <button class="secondary" onclick="saveToken()">Unlock Admin</button>
      </div>
      <div id="msg"></div>
    </div>

    <div id="adminContent" style="display:none;">
      <div class="panel">
        <h3 style="margin-top:0;">Add Facebook Fanpage</h3>
        <div class="row">
          <input id="url" placeholder="https://www.facebook.com/nasa or page name" />
          <input id="name" placeholder="Display name (optional)" style="max-width:260px;" />
          <button onclick="addFanpage()">+ Add &amp; Scrape</button>
        </div>
        <div id="actionMsg" style="margin-top:10px; font-size:14px; color:var(--muted);"></div>
      </div>

      <div class="panel">
        <h3 style="margin-top:0;">Tracked Facebook Fanpages (<span id="count">0</span>)</h3>
        <table>
          <thead><tr><th>Name</th><th>URL</th><th>Posts</th><th>Events</th><th>Last checked</th><th></th></tr></thead>
          <tbody id="rows"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const msg = (t, bad, id = 'msg') => { const m = document.getElementById(id); if (!m) return; m.textContent = t; m.style.color = bad ? '#c2476b' : '#2f8f6b'; };
    const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const headers = () => ({ 'Content-Type': 'application/json', 'X-Admin-Token': sessionStorage.getItem('adminToken') || '' });

    async function api(method, path, body) {
      const res = await fetch(path, { method, headers: headers(), body: body ? JSON.stringify(body) : undefined });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.detail || res.statusText);
      return data;
    }

    function saveToken() {
      const tok = document.getElementById('token').value.trim();
      if (!tok) return;
      sessionStorage.setItem('adminToken', tok);
      load();
    }

    function logoutAdmin() {
      sessionStorage.removeItem('adminToken');
      window.location.href = '/';
    }

    async function load() {
      try {
        const list = await api('GET', '/api/admin/fanpages');
        document.getElementById('unlockPanel').style.display = 'none';
        document.getElementById('adminContent').style.display = 'block';
        document.getElementById('scrapeAllBtn').style.display = 'inline-block';
        document.getElementById('logoutBtn').style.display = 'inline-block';
        document.getElementById('count').textContent = list.length;
        document.getElementById('rows').innerHTML = list.map(f => `
          <tr>
            <td><strong>${esc(f.name)}</strong><br><a href="/feeds/fanpages/${encodeURIComponent(f.slug)}/events.json" target="_blank" style="font-size:12px;">events.json ↗</a></td>
            <td><a href="${esc(f.url)}" target="_blank">${esc(f.url)}</a></td>
            <td>${f.post_count}</td>
            <td><span class="pill">${f.event_count}</span></td>
            <td>${esc(f.last_checked_at ? new Date(f.last_checked_at).toLocaleString() : '-')}</td>
            <td><button class="danger" data-id="${f.id}" data-name="${esc(f.name)}" onclick="removeFanpage(this)">Remove</button></td>
          </tr>`).join('');
      } catch (e) {
        document.getElementById('unlockPanel').style.display = 'block';
        document.getElementById('adminContent').style.display = 'none';
        document.getElementById('scrapeAllBtn').style.display = 'none';
        document.getElementById('logoutBtn').style.display = 'none';
        msg(e.message, true);
      }
    }

    async function scrapeAll() {
      try {
        await api('POST', '/api/admin/scrape');
        msg('Background scrape & extraction started across all tracked fanpages.', false, 'actionMsg');
      } catch (e) { msg(e.message, true, 'actionMsg'); }
    }

    async function addFanpage() {
      const url = document.getElementById('url').value.trim();
      const name = document.getElementById('name').value.trim() || null;
      if (!url) return;
      try {
        const r = await api('POST', '/api/admin/fanpages', { url, name });
        msg(r.created ? 'Added. Scraping started in background.' : 'Fanpage already tracked.', false, 'actionMsg');
        document.getElementById('url').value = ''; document.getElementById('name').value = '';
        load();
      } catch (e) { msg(e.message, true, 'actionMsg'); }
    }

    async function removeFanpage(btn) {
      if (!confirm('Remove "' + btn.dataset.name + '" and its posts, feed and events?')) return;
      try {
        await api('DELETE', '/api/admin/fanpages?identifier=' + encodeURIComponent(btn.dataset.id));
        msg('Removed.', false, 'actionMsg'); load();
      } catch (e) { msg(e.message, true, 'actionMsg'); }
    }

    if (sessionStorage.getItem('adminToken')) load();
  </script>
</body>
</html>
"""


@router.get("/admin", response_class=HTMLResponse)
def serve_admin_page():
    return HTMLResponse(content=ADMIN_HTML)
