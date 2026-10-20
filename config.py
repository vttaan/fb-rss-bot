from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Timezone Configuration (Default: Local system timezone / GMT+7)
LOCAL_TIMEZONE = datetime.now().astimezone().tzinfo or timezone(timedelta(hours=7))

# SQLite Database Configuration
DB_PATH = BASE_DIR / "rss_bot.db"
DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

# RSS XML Output Directory
FEEDS_DIR = BASE_DIR / "feeds"
FEEDS_DIR.mkdir(parents=True, exist_ok=True)

# Scraping & Scheduler Settings
DEFAULT_INTERVAL_SECONDS = 3600  # 1 hour
SCROLL_COUNT = 4                 # Number of scrolls per fanpage to load recent posts
SCROLL_DELAY_MS = 2500           # Wait time between scrolls in milliseconds
PAGE_TIMEOUT_MS = 45000          # Page navigation timeout
MAX_POSTS_IN_RSS = 50            # Max items to include in generated XML feed

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
