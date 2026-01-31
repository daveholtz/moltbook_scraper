#!/bin/bash
set -e

cd /Users/dholtz/Desktop/git_repos/moltbook_scraper

# Load environment
source .env
export MOLTBOOK_API_KEY

DATE=$(date +%Y-%m-%d)
LOG="logs/scrape-$DATE.log"

echo "[$DATE $(date +%H:%M:%S)] Starting daily scrape" >> "$LOG"

echo "[$DATE $(date +%H:%M:%S)] Scraping posts..." >> "$LOG"
python3 -m src.cli posts --db moltbook.db >> "$LOG" 2>&1

echo "[$DATE $(date +%H:%M:%S)] Scraping comments..." >> "$LOG"
python3 -m src.cli comments --db moltbook.db >> "$LOG" 2>&1

echo "[$DATE $(date +%H:%M:%S)] Enriching agents..." >> "$LOG"
python3 -m src.cli enrich --db moltbook.db >> "$LOG" 2>&1

echo "[$DATE $(date +%H:%M:%S)] Creating snapshots..." >> "$LOG"
python3 -m src.cli snapshots --db moltbook.db >> "$LOG" 2>&1

echo "[$DATE $(date +%H:%M:%S)] Daily scrape complete" >> "$LOG"
