#!/usr/bin/env bash
# Sync stocks.db from GitHub repo to local database
set -e

REPO_DIR="$HOME/projects/stock-updater"
DB_DEST="$HOME/.hermes/databases/stocks.db"

cd "$REPO_DIR"

# Pull latest from repo
git pull --ff-only origin main 2>/dev/null || git fetch origin main && git reset --hard origin/main

# Copy DB if it exists
if [ -f "$REPO_DIR/stocks.db" ]; then
    mkdir -p "$(dirname "$DB_DEST")"
    cp "$REPO_DIR/stocks.db" "$DB_DEST"
    echo "$(date '+%Y-%m-%d %H:%M') — stocks.db synced ($(du -h "$DB_DEST" | cut -f1))"
else
    echo "$(date '+%Y-%m-%d %H:%M') — no stocks.db in repo yet"
fi
