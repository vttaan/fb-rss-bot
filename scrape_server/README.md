# Facebook Fanpage RSS & WebSocket Scrape Server

A Python CLI & FastAPI server that scrapes newly uploaded posts from public Facebook Fanpages (no login required), stores and deduplicates them in **SQLite**, generates **RSS 2.0 `.xml` feeds**, and pushes real-time updates to users via **WebSockets** based on their subscribed fanpages every **1 hour**.

## Project Structure

- `main.py` — CLI entry point (`serve`, `add`, `list`, `remove`, `fetch`, `start`, `posts`, `export-rss`).
- `server.py` — Multi-user FastAPI + WebSocket + RSS server with a built-in Web Dashboard (`http://127.0.0.1:8000/`).
- `config.py` — Configuration for SQLite path (`rss_bot.db`), RSS feed output directory (`feeds/`), and 1-hour interval (`3600` seconds).
- `database/` — SQLAlchemy models (`User`, `UserSubscription`, `Fanpage`, `Post`) and persistence/deduplication helpers.
- `scraper/` — Headless Playwright scraper combining GraphQL/Relay JSON extraction with DOM fallback for public Facebook pages.
- `rss/` — RSS 2.0 `.xml` feed generator (`feeds/<page_slug>.xml`, `feeds/all.xml`, and per-user `feeds/user_<client_id>.xml`).
- `scheduler.py` — 1-hour background scheduler that invokes the scraper and triggers WebSocket push notifications.

## Running the Multi-User Scrape Server

```powershell
py main.py serve --host 127.0.0.1 --port 8000 --interval 3600
```

Once running:
- **Interactive Web Dashboard**: `http://127.0.0.1:8000/` (Switch User IDs, subscribe/unsubscribe fanpages, and watch live WebSocket updates)
- **WebSocket Endpoint**: `ws://127.0.0.1:8000/ws/{client_id}`
- **Personalized User RSS Feed (`.xml`)**: `http://127.0.0.1:8000/users/{client_id}/rss.xml`
- **Swagger / OpenAPI Docs**: `http://127.0.0.1:8000/docs`

### WebSocket Protocol (`ws://127.0.0.1:8000/ws/{client_id}`)

- **On Connect**: Server immediately sends `{"event": "init", "subscriptions": [...], "posts": [...]}` containing the user's selected fanpages and recent posts.
- **Subscribe to Fanpages** (Client -> Server):
  ```json
  {"action": "subscribe", "urls": ["https://www.facebook.com/nasa", "https://www.facebook.com/TechCrunch"]}
  ```
- **Unsubscribe** (Client -> Server):
  ```json
  {"action": "unsubscribe", "url": "https://www.facebook.com/nasa"}
  ```
- **Trigger Immediate Scrape** (Client -> Server):
  ```json
  {"action": "refresh"}
  ```
- **Real-Time New Posts Push** (Server -> Subscribed Clients Only):
  ```json
  {"event": "new_posts", "fanpage": {...}, "posts": [...]}
  ```

## Standalone CLI Usage

```powershell
py main.py add https://www.facebook.com/nasa --fetch
py main.py list
py main.py posts --limit 10
py main.py start --interval 3600
py main.py export-rss
```
