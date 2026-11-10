"""
Command-Line Interface (CLI) Streaming Demo for Real-Time Payment Fraud Engine.

Provides a live, interactive terminal monitor using the 'rich' library.
Run via:
    python run_cli.py
"""

import sys
import time
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from models import RiskStatus
from pipeline import FraudStreamingPipeline


console = Console()


def create_header(pipeline: FraudStreamingPipeline) -> Panel:
    metrics = pipeline.get_metrics()
    status_text = "[bold green]LIVE RUNNING[/bold green]" if metrics["is_running"] else "[bold yellow]PAUSED[/bold yellow]"
    
    header_content = (
        f"Stream Status: {status_text} | "
        f"Ingest Rate: [cyan]{metrics['current_tps']:.1f} txns/s[/cyan] | "
        f"Total Ingested: [bold white]{metrics['total_processed']}[/bold white] | "
        f"Fraud Intercepted: [bold red]{metrics['total_declined']}[/bold red] | "
        f"Fraud Rate: [bold yellow]{metrics['fraud_rate_pct']:.1f}%[/bold yellow] | "
        f"Avg Latency: [bold green]{metrics['avg_latency_ms']:.2f} ms[/bold green]"
    )
    return Panel(header_content, title="🛡️ StreamGuard | Real-Time Fraud Detection Engine", style="bold cyan")


def create_transactions_table(pipeline: FraudStreamingPipeline) -> Table:
    table = Table(title="📡 Live Ingest Stream (Recent Transactions)", expand=True)
    table.add_column("Time", style="dim", width=10)
    table.add_column("Txn ID", style="cyan", width=14)
    table.add_column("User ID", style="magenta", width=10)
    table.add_column("Amount", justify="right", style="green", width=10)
    table.add_column("Category", style="blue", width=16)
    table.add_column("Country", style="yellow", width=8)
    table.add_column("Score", justify="right", width=7)
    table.add_column("Status", justify="center", width=11)
    table.add_column("Triggered Explanation", style="white")

    with pipeline._lock:
        recent = list(pipeline.recent_results)[-12:]

    for r in reversed(recent):
        txn = r.transaction
        ts = time.strftime("%H:%M:%S", time.localtime(txn.timestamp))
        amt = f"${txn.amount:.2f}"
        score = f"{r.risk_score:.0f}"

        if r.status == RiskStatus.DECLINED:
            status_style = "[bold white on red] DECLINED [/bold white on red]"
        elif r.status == RiskStatus.REVIEW:
            status_style = "[bold black on yellow] REVIEW [/bold black on yellow]"
        else:
            status_style = "[bold white on green] APPROVED [/bold white on green]"

        table.add_row(
            ts,
            txn.transaction_id,
            txn.user_id,
            amt,
            txn.merchant_category,
            txn.location,
            score,
            status_style,
            r.primary_reason[:65] + ("..." if len(r.primary_reason) > 65 else ""),
        )

    return table


def main():
    console.clear()
    console.print("[bold cyan]Initializing StreamGuard Pipeline...[/bold cyan]")
    pipeline = FraudStreamingPipeline(tps=3.0, fraud_prob=0.20)
    pipeline.start()

    console.print("[bold green]Pipeline started. Press Ctrl+C to terminate.[/bold green]\n")
    time.sleep(1)

    try:
        with Live(console=console, screen=True, refresh_per_second=4) as live:
            while True:
                layout = Layout()
                layout.split_column(
                    Layout(create_header(pipeline), size=3),
                    Layout(create_transactions_table(pipeline)),
                )
                live.update(layout)
                time.sleep(0.25)
    except KeyboardInterrupt:
        pipeline.stop()
        console.print("\n[bold red]Stream paused and shut down cleanly.[/bold red]")


if __name__ == "__main__":
    main()
