import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent

# Auto-load .env file if present
def _load_env_file() -> None:
    env_path = BASE_DIR / ".env"
    if env_path.is_file():
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k and k not in os.environ:
                        os.environ[k] = v
        except Exception:
            pass

_load_env_file()

# Timezone Configuration (Asia/Ho_Chi_Minh / GMT+7)
try:
    SCRAPE_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:
    SCRAPE_TIMEZONE = timezone(timedelta(hours=7))

LOCAL_TIMEZONE = datetime.now().astimezone().tzinfo or SCRAPE_TIMEZONE

# SQLite Database Configuration
DB_PATH = BASE_DIR / "rss_bot.db"
DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

# RSS XML Output Directory
FEEDS_DIR = BASE_DIR / "feeds"
FEEDS_DIR.mkdir(parents=True, exist_ok=True)

# Public Ngrok Domain
PUBLIC_NGROK_DOMAIN = "stimulus-rectangle-ship.ngrok-free.dev"
PUBLIC_NGROK_URL = f"https://{PUBLIC_NGROK_DOMAIN}"

# Scraping & Scheduler Settings
DEFAULT_INTERVAL_SECONDS = 3600  # 1 hour (when explicitly using interval mode)
DEFAULT_SCRAPE_HOURS = "8,14,20"  # 3 times daily at 08:00, 14:00, 20:00 ICT
DEFAULT_SCRAPE_MINUTE = 0
SCRAPE_MISFIRE_GRACE_SECONDS = 900  # 15-minute grace window so delayed/paused runs still fire
SCROLL_COUNT = 4                 # Number of scrolls per fanpage to load recent posts
SCROLL_DELAY_MS = 2500           # Wait time between scrolls in milliseconds
PAGE_TIMEOUT_MS = 45000          # Page navigation timeout
MAX_POSTS_IN_RSS = 50            # Max items to include in generated XML feed
INITIAL_NEW_PAGE_POST_LIMIT = 5  # Fetch only the 5 most recent posts when scraping a newly added page

# Residential Proxy Configuration for Playwright Scraping
FB_PROXY_URL = os.environ.get("FB_PROXY_URL", "").strip()

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# Inbox Privacy, Safety Limits & CORS Settings
STORE_EMAIL_CONTENT = os.environ.get("STORE_EMAIL_CONTENT", "false").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
INGEST_MAX_BATCH_SIZE = int(os.environ.get("INGEST_MAX_BATCH_SIZE", "50"))
INGEST_MAX_CONTENT_CHARS = int(os.environ.get("INGEST_MAX_CONTENT_CHARS", "50000"))
INGEST_RATE_LIMIT_PER_MIN = int(os.environ.get("INGEST_RATE_LIMIT_PER_MIN", "60"))
CORS_ORIGINS = [
    o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()
]
GMAIL_WEB_CLIENT_ID = os.environ.get("GMAIL_WEB_CLIENT_ID", "").strip()
OUTLOOK_WEB_CLIENT_ID = os.environ.get("OUTLOOK_WEB_CLIENT_ID", "").strip()
