import asyncio
import sys
from typing import List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import typer
from rich.console import Console
from rich.table import Table

from config import DEFAULT_INTERVAL_SECONDS, FEEDS_DIR
from database.models import ensure_local_tz
from database import (
    add_fanpage,
    get_all_recent_posts,
    get_posts_for_fanpage,
    init_db,
    list_fanpages,
    remove_fanpage,
)
from rss import regenerate_all_feeds
from scheduler import run_scrape_cycle, start_scheduler_async

app = typer.Typer(
    name="fb-rss-bot",
    help="CLI Facebook Fanpage Scraper & RSS XML Feed Generator (1-hour auto refresh).",
    add_completion=False,
)
console = Console()


@app.command("add")
def cmd_add(
    urls: List[str] = typer.Argument(..., help="One or more Facebook fanpage URLs or usernames to track."),
    fetch_now: bool = typer.Option(False, "--fetch", "-f", help="Immediately scrape the added fanpage(s)."),
) -> None:
    """Add one or more public Facebook fanpage links to the SQLite database."""
    init_db()
    for raw_url in urls:
        try:
            fanpage, created = add_fanpage(raw_url)
            if created:
                console.print(
                    f"[green]+ Added fanpage:[/green] [bold]{fanpage.url}[/bold] "
                    f"(ID: {fanpage.id}, RSS: [cyan]feeds/{fanpage.slug}.xml[/cyan])"
                )
            else:
                console.print(
                    f"[yellow]! Fanpage already tracked:[/yellow] [bold]{fanpage.url}[/bold] (ID: {fanpage.id})"
                )
        except ValueError as exc:
            console.print(f"[red]x Invalid URL '{raw_url}': {exc}[/red]")

    if fetch_now:
        asyncio.run(run_scrape_cycle())


@app.command("list")
def cmd_list() -> None:
    """List all tracked Facebook fanpages in the database."""
    rows = list_fanpages(active_only=False)
    if not rows:
        console.print("[yellow]No fanpages tracked yet. Use `python main.py add <url>` to add one.[/yellow]")
        return

    table = Table(title="Tracked Facebook Fanpages", show_lines=True)
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("Name", style="bold")
    table.add_column("URL")
    table.add_column("Posts in DB", justify="right", style="green")
    table.add_column("Last Checked")
    table.add_column("RSS File", style="magenta")

    for fp, post_count in rows:
        lc = ensure_local_tz(fp.last_checked_at)
        last_checked = (
            lc.strftime("%Y-%m-%d %H:%M:%S %z")
            if lc
            else "Never"
        )
        table.add_row(
            str(fp.id),
            fp.name or fp.slug,
            fp.url,
            str(post_count),
            last_checked,
            f"feeds/{fp.slug}.xml",
        )

    console.print(table)


@app.command("remove")
def cmd_remove(
    identifier: str = typer.Argument(..., help="Fanpage ID, slug, or URL to remove."),
) -> None:
    """Remove a fanpage and its stored posts from the database."""
    removed = remove_fanpage(identifier)
    if removed:
        console.print(f"[green]Removed fanpage:[/green] [bold]{removed.url}[/bold] (ID: {removed.id})")
        regenerate_all_feeds()
    else:
        console.print(f"[red]No fanpage found matching '{identifier}'.[/red]")


@app.command("fetch")
def cmd_fetch(
    target: Optional[str] = typer.Argument(
        None, help="Optional fanpage URL, slug, or ID. If omitted, scrapes all active fanpages."
    ),
) -> None:
    """Run a one-time scrape immediately and update XML RSS feeds."""
    init_db()
    if target and ("facebook.com" in target or "fb.com" in target):
        add_fanpage(target)
    asyncio.run(run_scrape_cycle(target_slug_or_id=target))


@app.command("start")
def cmd_start(
    urls: Optional[List[str]] = typer.Argument(
        None, help="Optional fanpage URL(s) to add before starting the 1-hour scraper loop."
    ),
    interval: int = typer.Option(
        DEFAULT_INTERVAL_SECONDS,
        "--interval",
        "-i",
        help="Refresh interval in seconds (default: 3600 = 1 hour).",
    ),
) -> None:
    """Start the continuous RSS scraper bot (fetches every 1 hour by default)."""
    init_db()
    if urls:
        for raw_url in urls:
            try:
                fp, created = add_fanpage(raw_url)
                if created:
                    console.print(f"[green]+ Added fanpage:[/green] [bold]{fp.url}[/bold]")
            except ValueError as exc:
                console.print(f"[red]x Invalid URL '{raw_url}': {exc}[/red]")

    try:
        asyncio.run(start_scheduler_async(interval_seconds=interval))
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped by user.[/yellow]")


@app.command("posts")
def cmd_posts(
    fanpage_id: Optional[int] = typer.Option(None, "--page-id", "-p", help="Filter by Fanpage ID."),
    limit: int = typer.Option(15, "--limit", "-n", help="Number of recent posts to display."),
) -> None:
    """Display recently scraped posts stored in SQLite."""
    posts = (
        get_posts_for_fanpage(fanpage_id, limit=limit)
        if fanpage_id is not None
        else get_all_recent_posts(limit=limit)
    )
    if not posts:
        console.print("[yellow]No posts found in the database yet. Run `python main.py fetch` first.[/yellow]")
        return

    table = Table(title=f"Latest {len(posts)} Scraped Posts", show_lines=True)
    table.add_column("Post ID", style="dim", max_width=18)
    table.add_column("Fanpage", style="cyan")
    table.add_column("Published At", style="green")
    table.add_column("Content Snippet", max_width=55)
    table.add_column("Link", max_width=35)

    for post in posts:
        pub_dt = ensure_local_tz(post.published_at)
        pub_str = (
            pub_dt.strftime("%Y-%m-%d %H:%M %z")
            if pub_dt
            else "-"
        )
        snippet = (post.content or "[Media Only]").replace("\n", " ").strip()
        if len(snippet) > 110:
            snippet = snippet[:107] + "..."
        page_label = (post.fanpage.name or post.fanpage.slug) if post.fanpage else str(post.fanpage_id)
        table.add_row(post.post_id, page_label, pub_str, snippet, post.post_url)

    console.print(table)


@app.command("export-rss")
def cmd_export_rss() -> None:
    """Regenerate all .xml RSS feeds from the SQLite database without scraping."""
    paths = regenerate_all_feeds()
    console.print(f"[green]Generated {len(paths)} RSS XML feed file(s) in:[/green] [bold cyan]{FEEDS_DIR}[/bold cyan]")
    for p in paths:
        console.print(f"  - {p.name}")


@app.command("serve")
def cmd_serve(
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host address to bind."),
    port: int = typer.Option(8000, "--port", "-p", help="Port number to bind."),
    interval: int = typer.Option(
        DEFAULT_INTERVAL_SECONDS,
        "--interval",
        "-i",
        help="Background scrape refresh interval in seconds (default: 3600 = 1 hour).",
    ),
) -> None:
    """Host the FastAPI + WebSocket + RSS server for multi-user fanpage subscriptions."""
    import uvicorn
    import server

    server.SERVER_INTERVAL_SECONDS = interval
    console.print(
        f"[bold green]Starting Facebook Fanpage WebSocket & RSS Server[/bold green]\n"
        f"  Dashboard & Test UI : [underline cyan]http://{host}:{port}/[/underline cyan]\n"
        f"  WebSocket Endpoint  : [cyan]ws://{host}:{port}/ws/<client_id>[/cyan]\n"
        f"  User RSS XML Feed   : [cyan]http://{host}:{port}/users/<client_id>/rss.xml[/cyan]\n"
        f"  OpenAPI Docs        : [cyan]http://{host}:{port}/docs[/cyan]\n"
        f"  Scrape Interval     : {interval}s ({interval / 3600:.2f} hr)\n"
    )
    uvicorn.run(server.app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    app()
