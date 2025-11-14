#!/bin/bash
# Master Demo Script - Interactive stage-by-stage demo with Dagster UI
# For conference presentations showcasing StarRocks multi-catalog capabilities

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

show_menu() {
    clear
    echo ""
    echo "============================================================================"
    echo "  StarRocks Multi-Catalog Demo - Conference Presentation"
    echo "============================================================================"
    echo ""
    echo -e "${CYAN}This demo builds a data pipeline step-by-step, showing how StarRocks"
    echo -e "queries 3 different storage technologies through a single MySQL endpoint.${NC}"
    echo ""
    echo -e "  ${GREEN}[1]${NC} Run ALL stages (full interactive demo)"
    echo "  [0] Exit"
    echo ""
    read -p "Enter your choice: " choice
}

run_all_stages() {
    clear
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  Running Complete Interactive Demo - All 5 Stages                     ║${NC}"
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════════╗${NC}"
    echo ""
    echo -e "${CYAN}This will open Dagster UI for each asset."
    echo -e "You'll manually click 'Materialize' and see the results after each stage.${NC}"
    echo ""
    read -p "Press Enter to begin the demo..."

    # Run each stage
    bash "$SCRIPT_DIR/stage1_weather_data.sh"
    echo ""
    echo -e "${YELLOW}Stage 1 complete! Moving to Stage 2...${NC}"
    sleep 2

    bash "$SCRIPT_DIR/stage2_hive_catalog.sh"
    echo ""
    echo -e "${YELLOW}Stage 2 complete! Moving to Stage 3...${NC}"
    sleep 2

    bash "$SCRIPT_DIR/stage3_postgres_catalog.sh"
    echo ""
    echo -e "${YELLOW}Stage 3 complete! Moving to Stage 4...${NC}"
    sleep 2

    bash "$SCRIPT_DIR/stage4_ai_pipeline.sh"
    echo ""
    echo -e "${YELLOW}Stage 4 complete! Moving to Stage 5...${NC}"
    sleep 2

    bash "$SCRIPT_DIR/stage5_multi_catalog.sh"

    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  🎉 COMPLETE DEMO FINISHED!                                            ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════════════╝${NC}"
}

# Main loop
while true; do
    show_menu

    case $choice in
        1)
            run_all_stages
            read -p "Press Enter to return to menu..."
            ;;
        0)
            echo "Exiting demo script"
            exit 0
            ;;
        *)
            echo -e "${YELLOW}Invalid choice. Please try again.${NC}"
            sleep 1
            ;;
    esac
done
