"""CLI entry point for oscar-etl."""

import argparse
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from oscar_etl import __version__
from oscar_etl.etl import (
    DAILY_COLUMNS,
    EVENTS_COLUMNS,
    SESSION_COLUMNS,
    find_oscar_dir,
    scan_profiles,
    discover_sessions,
    parse_and_cache_edfs,
    etl_sessions,
    etl_daily,
    etl_events,
    etl_timeseries,
    write_csv,
)

console = Console()


def pick_profile(profiles):
    """Interactive profile picker when multiple profiles exist."""
    console.print()
    console.print(f"  Found {len(profiles)} profiles:")
    for i, p in enumerate(profiles, 1):
        console.print(f"    [{i}] {p['name']}  ({p['serial']})")
    console.print()
    while True:
        try:
            choice = input(f"  Which profile? [1]: ").strip()
            if not choice:
                return profiles[0]
            idx = int(choice) - 1
            if 0 <= idx < len(profiles):
                return profiles[idx]
        except (ValueError, EOFError):
            pass
        console.print("  Invalid choice, try again.")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="oscar-etl",
        description="Extract clean CSVs from your OSCAR CPAP data",
    )
    parser.add_argument(
        "--oscar-dir",
        help="Path to OSCAR_Data directory (auto-discovered if omitted)",
    )
    parser.add_argument(
        "--output-dir",
        default="./oscar-etl-output",
        help="Output directory (default: ./oscar-etl-output/)",
    )
    parser.add_argument(
        "--profile",
        help="Profile name (interactive picker if omitted and multiple exist)",
    )
    parser.add_argument(
        "--machine",
        help="Machine serial number filter",
    )
    parser.add_argument(
        "--skip-timeseries",
        action="store_true",
        help="Skip the large timeseries CSV (~800 MB)",
    )
    parser.add_argument(
        "--day-boundary",
        type=int,
        default=12,
        help="Hour (0-23) that separates sleep nights (default: 12)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"oscar-etl {__version__}",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    start_time = time.time()

    console.print()
    console.print(f"  [bold]oscar-etl[/bold] v{__version__}")
    console.print()

    # --- Discovery ---
    oscar_dir = find_oscar_dir(oscar_dir=args.oscar_dir)
    profiles = scan_profiles(
        oscar_dir,
        profile_name=args.profile,
        machine_serial=args.machine,
    )

    if len(profiles) > 1:
        profile = pick_profile(profiles)
    else:
        profile = profiles[0]

    datalog_dir = profile["datalog"]

    # Count EDF files
    edf_count = sum(1 for _ in datalog_dir.rglob("*.edf"))

    console.print("  [bold]Data source[/bold]")
    console.print(f"  {'─' * 35}")
    console.print(f"  Location:  {oscar_dir}")
    console.print(f"  Profile:   {profile['name']}")
    console.print(f"  Machine:   {profile['serial']}")
    console.print(f"  EDF files: {edf_count:,}")
    console.print()

    # --- Processing ---
    console.print("  [bold]Extracting[/bold]")
    console.print(f"  {'─' * 35}")

    output_dir = Path(args.output_dir)
    day_boundary = args.day_boundary

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        # Discover sessions
        task = progress.add_task("  Discovering sessions", total=1)
        sessions_by_date = discover_sessions(datalog_dir, day_boundary=day_boundary)
        total_sessions = sum(len(v) for v in sessions_by_date.values())
        progress.update(task, completed=1)

        # Parse EDF files
        task = progress.add_task("  Parsing EDF files", total=total_sessions)
        warnings = parse_and_cache_edfs(sessions_by_date, day_boundary=day_boundary)
        progress.update(task, completed=total_sessions)

        # Sessions CSV
        task = progress.add_task("  Writing sessions.csv", total=1)
        session_rows, unattributed = etl_sessions(sessions_by_date)
        write_csv(output_dir / "cpap_sessions.csv", SESSION_COLUMNS, session_rows)
        progress.update(task, completed=1)

        # Daily CSV
        task = progress.add_task("  Writing daily.csv", total=1)
        daily_rows = etl_daily(session_rows, sessions_by_date, unattributed)
        write_csv(output_dir / "cpap_daily.csv", DAILY_COLUMNS, daily_rows)
        progress.update(task, completed=1)

        # Events CSV
        task = progress.add_task("  Writing events.csv", total=1)
        event_rows = etl_events(sessions_by_date)
        write_csv(output_dir / "cpap_events.csv", EVENTS_COLUMNS, event_rows)
        progress.update(task, completed=1)

        # Timeseries CSV
        if not args.skip_timeseries:
            task = progress.add_task("  Writing timeseries.csv", total=1)
            ts_count = etl_timeseries(sessions_by_date, output_dir / "cpap_timeseries.csv")
            progress.update(task, completed=1)

    # --- Summary ---
    console.print()
    console.print("  [bold]Output[/bold]")
    console.print(f"  {'─' * 35}")
    console.print(f"  {output_dir}/")

    for name in ["cpap_sessions.csv", "cpap_daily.csv", "cpap_events.csv", "cpap_timeseries.csv"]:
        path = output_dir / name
        if path.exists():
            size = path.stat().st_size
            if size > 1024 * 1024:
                size_str = f"{size / (1024 * 1024):.1f} MB"
            elif size > 1024:
                size_str = f"{size / 1024:.0f} KB"
            else:
                size_str = f"{size} B"
            console.print(f"    {name:<25} {size_str}")

    if warnings:
        console.print()
        console.print(f"  [yellow]{len(warnings)} warning(s) during parsing[/yellow]")
        for w in warnings[:10]:
            console.print(f"    [dim]{w}[/dim]")
        if len(warnings) > 10:
            console.print(f"    [dim]...and {len(warnings) - 10} more[/dim]")

    elapsed = time.time() - start_time
    console.print()
    console.print(f"  Done in {elapsed:.0f}s")
    console.print()


if __name__ == "__main__":
    main()
