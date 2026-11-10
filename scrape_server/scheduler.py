import asyncio
import sys
from datetime import datetime, timedelta
from typing import Awaitable, Callable, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config import (
    DEFAULT_INTERVAL_SECONDS,
    DEFAULT_SCRAPE_HOURS,
    DEFAULT_SCRAPE_MINUTE,
    FEEDS_DIR,
    INITIAL_NEW_PAGE_POST_LIMIT,
    SCRAPE_MISFIRE_GRACE_SECONDS,
    SCRAPE_TIMEZONE,
)
from database import (
    Fanpage,
    Post,
    list_fanpages,
    save_posts,
    update_fanpage_metadata,
)
from rss import generate_all_rss, generate_fanpage_rss
from events import analyze_fanpage_posts, sync_all_event_folders
from scraper import scrape_fanpage

console = Console()
NewPostsCallback = Callable[[Fanpage, List[Post]], Awaitable[None]]


async def run_scrape_cycle(
    target_slug_or_id: Optional[str] = None,
    on_new_posts: Optional[NewPostsCallback] = None,
) -> int:
    """
    Execute one full scrape & RSS generation cycle across active fanpages.
    Returns total number of newly discovered posts.
    """
    rows = list_fanpages(active_only=True)
    if target_slug_or_id:
        rows = [
            (fp, count)
            for fp, count in rows
            if str(fp.id) == target_slug_or_id
            or fp.slug.lower() == target_slug_or_id.lower()
            or target_slug_or_id.lower() in fp.url.lower()
        ]

    if not rows:
        console.print(
            "[yellow]No matching active fanpages found in database. "
            "Add one first with:[/yellow] [bold cyan]python main.py add <fanpage_url>[/bold cyan]"
        )
        return 0

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    console.print(
        Panel(
            f"[bold cyan]Starting Facebook Fanpage RSS Scrape Cycle[/bold cyan]\n"
            f"Time: {now_str} | Fanpages to check: [bold]{len(rows)}[/bold]",
            expand=False,
        )
    )

    total_new_posts = 0
    summary_table = Table(title="Scrape Cycle Summary", show_lines=True)
    summary_table.add_column("ID", style="dim", justify="right")
    summary_table.add_column("Fanpage", style="bold")
    summary_table.add_column("Fetched", justify="right")
    summary_table.add_column("New Posts", justify="right", style="green")
    summary_table.add_column("RSS XML File", style="cyan")
    summary_table.add_column("Status")

    for idx, (fp, existing_count) in enumerate(rows):
        is_new_page = (fp.last_checked_at is None) or (existing_count == 0)
        console.print(f"[bold blue]-> [{idx + 1}/{len(rows)}] Scraping:[/bold blue] {fp.url} ...")
        try:
            page_title, scraped_items = await scrape_fanpage(fp.url)
            if is_new_page and len(scraped_items) > INITIAL_NEW_PAGE_POST_LIMIT:
                scraped_items = scraped_items[:INITIAL_NEW_PAGE_POST_LIMIT]
                console.print(
                    f"[cyan]   New fanpage: keeping {len(scraped_items)} most recent post(s).[/cyan]"
                )
            update_fanpage_metadata(fp.id, name=page_title)
            if page_title:
                fp.name = page_title

            new_posts = save_posts(fp.id, scraped_items)
            total_new_posts += len(new_posts)

            xml_path = generate_fanpage_rss(fp)

            try:
                ev = await asyncio.to_thread(analyze_fanpage_posts, fp.id)
                console.print(
                    f"[magenta]   Events: {ev['events']} found, {ev['llm_calls']} LLM call(s), "
                    f"{ev['analyzed']} post(s) analyzed[/magenta]"
                )
            except Exception as ev_exc:
                console.print(f"[yellow]   Event analysis skipped: {ev_exc}[/yellow]")
            if new_posts and on_new_posts is not None:
                try:
                    await on_new_posts(fp, new_posts)
                except Exception as cb_exc:
                    console.print(f"[yellow]   Warning in WebSocket push callback: {cb_exc}[/yellow]")

            summary_table.add_row(
                str(fp.id),
                fp.name or fp.slug,
                str(len(scraped_items)),
                f"+{len(new_posts)}",
                xml_path.name,
                "[green]OK[/green]",
            )
        except Exception as exc:
            console.print(f"[red]   Error scraping {fp.url}: {exc}[/red]")
            summary_table.add_row(
                str(fp.id),
                fp.name or fp.slug,
                "0",
                "0",
                "-",
                f"[red]Error: {str(exc)[:50]}[/red]",
            )

        if idx < len(rows) - 1:
            await asyncio.sleep(3.5)

    all_xml_path = generate_all_rss()
    try:
        await asyncio.to_thread(sync_all_event_folders)
    except Exception as sync_exc:
        console.print(f"[yellow]Warning syncing event folders: {sync_exc}[/yellow]")
    console.print(summary_table)
    console.print(
        f"[bold green]Cycle complete![/bold green] Added [bold]{total_new_posts}[/bold] new post(s). "
        f"Combined RSS feed saved to: [underline cyan]{all_xml_path}[/underline cyan] "
        f"(Folder: [cyan]{FEEDS_DIR}[/cyan])\n"
    )
    return total_new_posts


def parse_scrape_hours(hours_str: str = DEFAULT_SCRAPE_HOURS) -> List[int]:
    """Parse a comma-separated list of hours (0-23), falling back to [8, 14, 20]."""
    hours: List[int] = []
    for part in (hours_str or "").split(","):
        part = part.strip()
        if not part:
            continue
        val = int(part)
        if 0 <= val <= 23 and val not in hours:
            hours.append(val)
    return sorted(hours) if hours else [8, 14, 20]


def build_daily_cron_trigger(
    hours_str: str = DEFAULT_SCRAPE_HOURS,
    minute: int = DEFAULT_SCRAPE_MINUTE,
) -> CronTrigger:
    """Build an APScheduler CronTrigger locked to `Asia/Ho_Chi_Minh` (UTC+7)."""
    hours = parse_scrape_hours(hours_str)
    return CronTrigger(
        hour=",".join(str(h) for h in hours),
        minute=minute,
        second=0,
        timezone=SCRAPE_TIMEZONE,
    )


async def _scheduled_cron_job(hours_str: str, minute: int) -> None:
    await run_scrape_cycle()
    trigger = build_daily_cron_trigger(hours_str=hours_str, minute=minute)
    next_run = trigger.get_next_fire_time(None, datetime.now(SCRAPE_TIMEZONE))
    if next_run:
        console.print(
            f"[dim]Next scheduled fetch at {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')} "
            f"(3x daily). Press Ctrl+C to stop.[/dim]"
        )


async def _scheduled_job(interval_seconds: int) -> None:
    await run_scrape_cycle()
    next_run = datetime.now(SCRAPE_TIMEZONE) + timedelta(seconds=interval_seconds)
    console.print(
        f"[dim]Next automatic fetch scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')} "
        f"(in {interval_seconds // 60} mins). Press Ctrl+C to stop.[/dim]"
    )


async def start_scheduler_async(
    interval_seconds: Optional[int] = None,
    hours_str: str = DEFAULT_SCRAPE_HOURS,
    minute: int = DEFAULT_SCRAPE_MINUTE,
    run_immediately: bool = True,
) -> None:
    """
    Start the scraper daemon.
    - Default (when `interval_seconds` is None): runs 3x daily at `hours_str` (08:00, 14:00, 20:00 ICT).
    - If `interval_seconds` is explicitly provided: runs on a fixed second interval.
    """
    scheduler = AsyncIOScheduler(timezone=SCRAPE_TIMEZONE)

    if interval_seconds is not None:
        console.print(
            Panel(
                f"[bold green]Facebook RSS Bot Daemon Started (Interval Mode)[/bold green]\n"
                f"Refresh Interval: [bold]{interval_seconds} seconds ({interval_seconds / 3600:.2f} hour(s))[/bold]\n"
                f"RSS Output Directory: [cyan]{FEEDS_DIR}[/cyan]\n"
                f"Press [bold]Ctrl+C[/bold] at any time to stop.",
                expand=False,
            )
        )
        if run_immediately:
            await _scheduled_job(interval_seconds)
        scheduler.add_job(
            _scheduled_job,
            trigger="interval",
            seconds=interval_seconds,
            args=[interval_seconds],
            id="fb_rss_scrape_job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=SCRAPE_MISFIRE_GRACE_SECONDS,
        )
    else:
        parsed_hours = parse_scrape_hours(hours_str)
        schedule_label = ", ".join(f"{h:02d}:{minute:02d}" for h in parsed_hours) + " ICT (UTC+7)"
        console.print(
            Panel(
                f"[bold green]Facebook RSS Bot Daemon Started (Daily Cron Mode)[/bold green]\n"
                f"Scheduled Times: [bold]{schedule_label}[/bold]\n"
                f"RSS Output Directory: [cyan]{FEEDS_DIR}[/cyan]\n"
                f"Press [bold]Ctrl+C[/bold] at any time to stop.",
                expand=False,
            )
        )
        if run_immediately:
            await _scheduled_cron_job(hours_str, minute)
        scheduler.add_job(
            _scheduled_cron_job,
            trigger=build_daily_cron_trigger(hours_str=hours_str, minute=minute),
            args=[hours_str, minute],
            id="fb_rss_scrape_job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=SCRAPE_MISFIRE_GRACE_SECONDS,
        )

    scheduler.start()

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.print("\n[yellow]Shutting down scheduler...[/yellow]")
        scheduler.shutdown(wait=False)
