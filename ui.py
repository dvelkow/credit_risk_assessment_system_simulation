import time
from typing import List
import sys
import os

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.text import Text
from rich.align import Align
from rich.columns import Columns
from rich import box

from models import Profile
from assessment import RISK_THRESHOLDS

RISK_COLORS = {
    "Excellent": "bright_green",
    "Good": "green",
    "Fair": "yellow",
    "Poor": "red",
    "Very Poor": "bright_red",
}

# Force UTF-8 output on Windows
if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

console = Console()

def show_banner():
    banner = r"""
  ____              _ _ _     ____  _     _
 / ___|_ __ ___  __| (_) |_  |  _ \(_)___| | __
| |   | '__/ _ \/ _` | | __| | |_) | / __| |/ /
| |___| | |  __/ (_| | | |_  |  _ <| \__ \   <
 \____|_|  \___|\__,_|_|\__| |_| \_\_|___/_|\_\
"""
    banner_text = Text()
    colors = ["#00ffff", "#00dfff", "#00bfff", "#009fff", "#007fff", "#005fff"]
    for i, line in enumerate(banner.strip("\n").split("\n")):
        banner_text.append(line + "\n", style=f"bold {colors[i % len(colors)]}")

    console.print(Panel(
        Align.center(banner_text),
        title="[bold white]🌟 Assessment System Simulation 🌟[/]",
        subtitle="[bold bright_magenta]v3.0 - Terminal Edition[/]",
        border_style="bright_cyan",
        box=box.HEAVY,
        padding=(1, 2)
    ))
    console.print()

def display_generation_progress(num_profiles: int):
    """Shows a quick loading bar for generated or loaded profiles."""
    with Progress(
        SpinnerColumn("point", style="bold cyan"),
        TextColumn("[bold bright_cyan]Loading profiles...[/]"),
        BarColumn(bar_width=40, complete_style="cyan", finished_style="bright_green", pulse_style="bright_white"),
        TextColumn("[bold white][progress.percentage]{task.percentage:>3.0f}%[/]"),
        TextColumn("•", style="dim"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Generating", total=num_profiles)
        for i in range(num_profiles):
            progress.update(task, advance=1)
            time.sleep(0.01) # Faster than before
    console.print(f"  [green][+][/] {num_profiles} customer profiles ready.\n")

def display_assessment_progress(profiles: List[Profile], assess_func):
    """Shows a progress bar while assessing profiles, returning the assessed list."""
    assessed = []
    with Progress(
        SpinnerColumn("point", style="bold magenta"),
        TextColumn("[bold bright_magenta]Assessing and underwriting risk...[/]"),
        BarColumn(bar_width=40, complete_style="magenta", finished_style="bright_green", pulse_style="bright_white"),
        TextColumn("[bold white][progress.percentage]{task.percentage:>3.0f}%[/]"),
        TextColumn("•", style="dim"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Assessing", total=len(profiles))
        for p in profiles:
            assessed.append(assess_func(p))
            progress.update(task, advance=1)
            time.sleep(0.01)
    console.print(f"  [green][+][/] All {len(profiles)} profiles assessed.\n")
    return assessed

def build_main_table(profiles: List[Profile]) -> Table:
    table = Table(
        title="[bold bright_white]📊 Customer Risk Assessment Report 📊[/]",
        box=box.HEAVY_EDGE,
        show_lines=False,
        header_style="bold bright_white on #000080",
        border_style="bright_blue",
        row_styles=["", "dim"],
        pad_edge=True,
        expand=True,
    )

    table.add_column("ID", style="dim", justify="right")
    table.add_column("Name", no_wrap=True)
    table.add_column("Req Loan", justify="right")
    table.add_column("Purpose")
    table.add_column("Income", justify="right")
    table.add_column("Credit", justify="center")
    table.add_column("Late", justify="center")
    table.add_column("BK", justify="center")
    table.add_column("Score", justify="center")
    table.add_column("Risk", justify="center", no_wrap=True)
    table.add_column("Limit", justify="right")
    table.add_column("Reason", style="italic cyan")

    for p in profiles:
        color = RISK_COLORS.get(p.risk_category, "white")

        # Color-code credit score
        if p.credit_score >= 750:
            cs_color = "bright_green"
        elif p.credit_score >= 670:
            cs_color = "green"
        elif p.credit_score >= 580:
            cs_color = "yellow"
        else:
            cs_color = "red"
            
        # Color-code bankruptcies/lates
        bk_color = "red" if p.recent_bankruptcies > 0 else "dim"
        late_color = "yellow" if p.missed_payments_history > 0 else "dim"

        risk_emoji = {
            "Excellent": "🌟",
            "Good": "✅",
            "Fair": "⚠️",
            "Poor": "❌",
            "Very Poor": "💀"
        }.get(p.risk_category, "")

        table.add_row(
            str(p.id),
            p.name,
            f"${p.loan_amount_requested:,.0f}",
            p.loan_purpose,
            f"${p.annual_income:,.0f}",
            f"[{cs_color}]{p.credit_score}[/]",
            f"[{late_color}]{p.missed_payments_history}[/]",
            f"[{bk_color}]{p.recent_bankruptcies}[/]",
            f"[bold {color}]{p.risk_score:.0f}[/]",
            f"[{color}]{risk_emoji} {p.risk_category}[/]",
            f"${p.credit_limit:,.0f}",
            p.reason,
        )

    return table

def show_table_animated(profiles: List[Profile]):
    table = build_main_table(profiles)
    console.print(table)
    console.print()

def show_summary(profiles: List[Profile]):
    if not profiles:
        return
        
    scores = [p.risk_score for p in profiles]
    incomes = [p.annual_income for p in profiles]
    credit_scores = [p.credit_score for p in profiles]
    dtis = [p.dti_ratio for p in profiles]
    req_loans = [p.loan_amount_requested for p in profiles]

    # Count by category
    categories = {}
    for p in profiles:
        categories[p.risk_category] = categories.get(p.risk_category, 0) + 1

    # Summary stats panel
    stats_text = Text()
    stats_text.append("  Profiles Assessed:  ", style="dim")
    stats_text.append(f"{len(profiles)}\n", style="bold white")
    stats_text.append("  Avg Risk Score:     ", style="dim")
    stats_text.append(f"{sum(scores)/len(scores):.1f} / 100\n", style="bold cyan")
    stats_text.append("  Avg Credit Score:   ", style="dim")
    stats_text.append(f"{sum(credit_scores)/len(credit_scores):.0f}\n", style="bold white")
    stats_text.append("  Avg Annual Income:  ", style="dim")
    stats_text.append(f"${sum(incomes)/len(incomes):,.0f}\n", style="bold white")
    stats_text.append("  Avg Loan Requested: ", style="dim")
    stats_text.append(f"${sum(req_loans)/len(req_loans):,.0f}\n", style="bold white")
    stats_text.append("  Avg DTI Ratio:      ", style="dim")
    stats_text.append(f"{sum(dtis)/len(dtis):.1f}%\n", style="bold white")
    stats_text.append("  Highest Score:      ", style="dim")
    stats_text.append(f"{max(scores):.1f}\n", style="bold bright_green")
    stats_text.append("  Lowest Score:       ", style="dim")
    stats_text.append(f"{min(scores):.1f}", style="bold bright_red")

    stats_panel = Panel(
        stats_text,
        title="[bold]📈 Summary Statistics[/]",
        border_style="cyan",
        box=box.HEAVY_EDGE,
        padding=(1, 2)
    )

    # Risk distribution panel
    bar_width = 30
    order = ["Excellent", "Good", "Fair", "Poor", "Very Poor"]
    dist_text = Text()
    for cat in order:
        count = categories.get(cat, 0)
        pct = (count / len(profiles)) * 100
        filled = int((pct / 100) * bar_width)
        color = RISK_COLORS[cat]
        bar = "█" * filled + "░" * (bar_width - filled)
        dist_text.append(f"  {cat:<12} ", style="dim")
        dist_text.append(bar, style=color)
        dist_text.append(f" {count:>2} ({pct:>4.1f}%)\n", style="bold white")

    dist_panel = Panel(
        dist_text,
        title="[bold]📊 Risk Distribution[/]",
        border_style="magenta",
        box=box.HEAVY_EDGE,
        padding=(1, 2)
    )

    console.print(Columns([stats_panel, dist_panel], equal=True, expand=True))
    console.print()

def show_top_bottom(profiles: List[Profile]):
    if not profiles:
        return
        
    sorted_profiles = sorted(profiles, key=lambda p: p.risk_score, reverse=True)

    # Top 5 safest
    top_table = Table(
        title="[bold bright_green]🏆 Top 5 Safest Clients 🏆[/]",
        box=box.SIMPLE_HEAVY,
        header_style="bold black on bright_green",
        border_style="bright_green",
    )
    top_table.add_column("Name", min_width=20)
    top_table.add_column("Score", justify="center")
    top_table.add_column("Credit", justify="center")
    top_table.add_column("Income", justify="right")
    top_table.add_column("Approved", justify="right")

    for p in sorted_profiles[:5]:
        top_table.add_row(
            p.name,
            f"[bold bright_green]{p.risk_score:.1f}[/]",
            str(p.credit_score),
            f"${p.annual_income:,.0f}",
            f"${p.credit_limit:,.0f}"
        )

    # Bottom 5 riskiest (avoid exposing internal bounds if less than 5 people)
    bot_amount = min(5, len(profiles))
    bot_table = Table(
        title="[bold bright_red]⚠️ Top 5 Riskiest Clients ⚠️[/]",
        box=box.SIMPLE_HEAVY,
        header_style="bold white on bright_red",
        border_style="bright_red",
    )
    bot_table.add_column("Name", min_width=20)
    bot_table.add_column("Score", justify="center")
    bot_table.add_column("Credit", justify="center")
    bot_table.add_column("Bkrtys/Lates", justify="center")
    bot_table.add_column("Approved", justify="right")

    for p in sorted_profiles[-bot_amount:]:
        bot_table.add_row(
            p.name,
            f"[bold bright_red]{p.risk_score:.1f}[/]",
            str(p.credit_score),
            f"{p.recent_bankruptcies} / {p.missed_payments_history}",
            f"${p.credit_limit:,.0f}",
        )

    console.print(Columns([top_table, bot_table], equal=True, expand=True))
    console.print()

def show_approval_simulation(profiles: List[Profile]):
    approved = [p for p in profiles if p.risk_score >= 50]
    denied = [p for p in profiles if p.risk_score < 30]
    review = [p for p in profiles if 30 <= p.risk_score < 50]

    total_credit_requested = sum(p.loan_amount_requested for p in profiles)
    total_credit_granted = sum(p.credit_limit for p in profiles)

    result = Text()
    result.append("  ✅ ")
    result.append("Approved:     ", style="bold bright_green")
    result.append(f"{len(approved)} clients\n", style="bold white")
    result.append("  ❌ ")
    result.append("Denied:       ", style="bold bright_red")
    result.append(f"{len(denied)} clients\n", style="bold white")
    result.append("  ⚠️  ")
    result.append("Under Review: ", style="bold bright_yellow")
    result.append(f"{len(review)} clients\n\n", style="bold white")
    result.append("  Total Funds Requested:  ", style="dim")
    result.append(f"${total_credit_requested:,.0f}\n", style="bold bright_blue")
    result.append("  Total Credit Extended: ", style="dim")
    result.append(f"${total_credit_granted:,.0f}", style="bold bright_green")

    console.print(Panel(
        result,
        title="[bold]🏦 Loan Decision Simulation 🏦[/]",
        border_style="bright_blue",
        box=box.HEAVY,
        padding=(1, 2)
    ))
    console.print()
