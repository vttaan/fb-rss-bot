import asyncio
import sys
from datetime import datetime, timedelta
from typing import Awaitable, Callable, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config import DEFAULT_INTERVAL_SECONDS, FEEDS_DIR
from database import (
    Fanpage,
    Post,
    list_fanpages,
    save_posts,
    update_fanpage_metadata,
)
from rss import generate_all_rss, generate_fanpage_rss
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

    for idx, (fp, _) in enumerate(rows):
        console.print(f"[bold blue]-> [{idx + 1}/{len(rows)}] Scraping:[/bold blue] {fp.url} ...")
        try:
            page_title, scraped_items = await scrape_fanpage(fp.url)
            update_fanpage_metadata(fp.id, name=page_title)
            if page_title:
                fp.name = page_title

            new_posts = save_posts(fp.id, scraped_items)
            total_new_posts += len(new_posts)

            xml_path = generate_fanpage_rss(fp)
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
    console.print(summary_table)
    console.print(
        f"[bold green]Cycle complete![/bold green] Added [bold]{total_new_posts}[/bold] new post(s). "
        f"Combined RSS feed saved to: [underline cyan]{all_xml_path}[/underline cyan] "
        f"(Folder: [cyan]{FEEDS_DIR}[/cyan])\n"
    )
    return total_new_posts


async def _scheduled_job(interval_seconds: int) -> None:
    await run_scrape_cycle()
    next_run = datetime.now() + timedelta(seconds=interval_seconds)
    console.print(
        f"[dim]Next automatic fetch scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
        f"(in {interval_seconds // 60} mins). Press Ctrl+C to stop.[/dim]"
    )


async def start_scheduler_async(interval_seconds: int = DEFAULT_INTERVAL_SECONDS) -> None:
    """Run an immediate scrape cycle and then schedule recurring runs every `interval_seconds` (default 1 hour)."""
    console.print(
        Panel(
            f"[bold green]Facebook RSS Bot Daemon Started[/bold green]\n"
            f"Refresh Interval: [bold]{interval_seconds} seconds ({interval_seconds / 3600:.2f} hour(s))[/bold]\n"
            f"RSS Output Directory: [cyan]{FEEDS_DIR}[/cyan]\n"
            f"Press [bold]Ctrl+C[/bold] at any time to stop.",
            expand=False,
        )
    )

    # Run first fetch immediately
    await _scheduled_job(interval_seconds)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _scheduled_job,
        trigger="interval",
        seconds=interval_seconds,
        args=[interval_seconds],
        id="fb_rss_scrape_job",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.print("\n[yellow]Shutting down scheduler...[/yellow]")
        scheduler.shutdown(wait=False)
