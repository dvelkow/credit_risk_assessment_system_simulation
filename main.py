#!/usr/bin/env python3
"""
Credit Risk Assessment System
─────────────────────────────
Ingests real CSV applicant data or generates a simulation, 
assesses credit risk using a scorecard model, and outputs beautiful terminal analytics.
"""

import argparse
import time

from generator import generate_profile
from assessment import assess_risk
from data_io import load_profiles_from_csv, export_profiles_to_csv
import ui

def main():
    parser = argparse.ArgumentParser(description="Credit Risk Assessment System")
    parser.add_argument("--simulate", type=int, metavar="NUM", 
                        help="Run a simulation generating NUM random profiles (Default: 50)")
    parser.add_argument("--input", type=str, metavar="FILE", 
                        help="Process real applicant data from a CSV file")
    parser.add_argument("--output", type=str, metavar="FILE", 
                        help="Export the assessed results to a CSV file")
    
    args = parser.parse_args()

    ui.console.clear()
    ui.show_banner()
    time.sleep(0.5)

    profiles = []

    # Phase 1: Ingestion
    if args.input:
        ui.console.rule("[bold bright_cyan]⚡ Phase 1: Data Ingestion ⚡[/]", style="bright_cyan", characters="━")
        ui.console.print()
        profiles = load_profiles_from_csv(args.input)
        ui.display_generation_progress(len(profiles))
    else:
        # Default to simulation if no input provided
        num_profiles = args.simulate if args.simulate else 50
        ui.console.rule("[bold bright_cyan]⚡ Phase 1: Profile Simulation ⚡[/]", style="bright_cyan", characters="━")
        ui.console.print()
        ui.display_generation_progress(num_profiles)
        profiles = [generate_profile(i + 1) for i in range(num_profiles)]
        
    if not profiles:
        ui.console.print("[red]No profiles to process. Exiting.[/]")
        return

    # Phase 2: Risk assessment
    ui.console.rule("[bold bright_magenta]🔍 Phase 2: Risk Assessment 🔍[/]", style="bright_magenta", characters="━")
    ui.console.print()
    assessed_profiles = ui.display_assessment_progress(profiles, assess_risk)

    # Output Export (optional non-UI step)
    if args.output:
        export_profiles_to_csv(assessed_profiles, args.output)
        ui.console.print(f"  [green][+][/] Results exported to [bold]{args.output}[/]\n")

    # Phase 3: Results
    ui.console.rule("[bold bright_green]📊 Phase 3: Assessment Report 📊[/]", style="bright_green", characters="━")
    ui.console.print()
    ui.show_table_animated(assessed_profiles)

    # Phase 4: Analytics
    ui.console.rule("[bold bright_yellow]📈 Phase 4: Analytics & Summary 📈[/]", style="bright_yellow", characters="━")
    ui.console.print()
    ui.show_summary(assessed_profiles)
    ui.show_top_bottom(assessed_profiles)

    # Phase 5: Loan decisions
    ui.console.rule("[bold bright_blue]🏦 Phase 5: Loan Decision Simulation 🏦[/]", style="bright_blue", characters="━")
    ui.console.print()
    ui.show_approval_simulation(assessed_profiles)

    ui.console.rule("[bold dim]✅ Assessment Complete ✅[/]", style="dim", characters="━")
    ui.console.print()


if __name__ == "__main__":
    main()
