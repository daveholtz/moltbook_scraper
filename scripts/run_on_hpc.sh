#!/bin/bash
#
# Moltbook scraper HPC job script
# Usage: grid_run --grid_submit=batch --grid_mem=8G ./scripts/run_on_hpc.sh
#
# Options (set via environment variables):
#   MOLTBOOK_CONTINUOUS=1    Run continuously (loop forever with sleep between runs)
#   MOLTBOOK_SLEEP=3600      Seconds to sleep between runs (default: 1 hour)
#   MOLTBOOK_DB=moltbook.db  Database file path
#

set -e

# Initialize conda for non-interactive shell
source /apps/anaconda3/etc/profile.d/conda.sh
conda activate moltbook

# Change to repo directory
cd ~/moltbook_scraper

# Configuration with defaults
DB_FILE="${MOLTBOOK_DB:-moltbook.db}"
CONTINUOUS="${MOLTBOOK_CONTINUOUS:-0}"
SLEEP_SECONDS="${MOLTBOOK_SLEEP:-3600}"
LOG_DIR="logs"

# Create logs directory
mkdir -p "$LOG_DIR"

# Log file with timestamp
LOG_FILE="$LOG_DIR/scrape_$(date +%Y%m%d_%H%M%S).log"

echo "========================================" | tee -a "$LOG_FILE"
echo "Moltbook scraper starting at $(date)" | tee -a "$LOG_FILE"
echo "Database: $DB_FILE" | tee -a "$LOG_FILE"
echo "Continuous mode: $CONTINUOUS" | tee -a "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

run_scrape() {
    echo "" | tee -a "$LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting scrape run..." | tee -a "$LOG_FILE"

    # Pull latest code
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pulling latest code..." | tee -a "$LOG_FILE"
    git pull 2>&1 | tee -a "$LOG_FILE"

    # Back up database before run (migration adds new columns on init)
    if [ -f "$DB_FILE" ]; then
        BACKUP_FILE="${DB_FILE}.backup_$(date +%Y%m%d_%H%M%S)"
        cp "$DB_FILE" "$BACKUP_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Database backed up to $BACKUP_FILE" | tee -a "$LOG_FILE"
    fi

    # Run with unbuffered output, capturing to log and stdout
    PYTHONUNBUFFERED=1 python -m src.cli full --db "$DB_FILE" 2>&1 | tee -a "$LOG_FILE"

    local exit_code=${PIPESTATUS[0]}
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scrape finished with exit code: $exit_code" | tee -a "$LOG_FILE"
    return $exit_code
}

if [ "$CONTINUOUS" = "1" ]; then
    echo "Running in continuous mode (sleep $SLEEP_SECONDS seconds between runs)" | tee -a "$LOG_FILE"
    while true; do
        run_scrape || echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scrape failed, will retry after sleep" | tee -a "$LOG_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sleeping for $SLEEP_SECONDS seconds..." | tee -a "$LOG_FILE"
        sleep "$SLEEP_SECONDS"
    done
else
    run_scrape
fi
