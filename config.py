from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent

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

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
