#!/usr/bin/env bash
# Sync stocks.db from CachyOS laptop → VPS
set -e

LAPTOP="cachyos"
LAPTOP_DB='~/.hermes/databases/stocks.db'
VPS_DB="$HOME/.hermes/databases/stocks.db"
SUMMARY="$HOME/projects/stock-updater/updated_stocks.json"

echo "=== Stock Sync $(date '+%Y-%m-%d %H:%M') ==="

# Test SSH connection
if ! ssh "$LAPTOP" "test -f $LAPTOP_DB" 2>/dev/null; then
    echo "⚠️  Laptop not reachable or DB not found"
    exit 1
fi

# Get remote file info
REMOTE_INFO=$(ssh "$LAPTOP" "ls -la $LAPTOP_DB && python3 -c \"
import sqlite3
conn = sqlite3.connect('$LAPTOP_DB')
r = conn.execute('SELECT MAX(date), COUNT(DISTINCT symbol), COUNT(*) FROM stock_ohlcv').fetchone()
conn.close()
print(f'Latest: {r[0]}, Stocks: {r[1]}, Rows: {r[2]}')
\"" 2>/dev/null)
echo "📡 Laptop: $REMOTE_INFO"

# Sync DB (use rsync for efficiency)
mkdir -p "$(dirname "$VPS_DB")"
rsync -avz --progress "$LAPTOP:$LAPTOP_DB" "$VPS_DB" 2>/dev/null

# Verify
python3 -c "
import sqlite3
conn = sqlite3.connect('$VPS_DB')
r = conn.execute('SELECT MAX(date), COUNT(DISTINCT symbol), COUNT(*) FROM stock_ohlcv').fetchone()
conn.close()
print(f'✅ VPS DB: {r[1]} stocks, {r[2]:,} rows, latest: {r[0]}')
" 2>/dev/null

# Save summary
python3 -c "
import sqlite3, json, os
conn = sqlite3.connect('$VPS_DB')
r = conn.execute('SELECT MAX(date), COUNT(DISTINCT symbol), COUNT(*) FROM stock_ohlcv').fetchone()
conn.close()
result = {
    'timestamp': '$(date -u +%Y-%m-%dT%H:%M:%SZ)',
    'latest_date': r[0],
    'total_stocks': r[1],
    'total_rows': r[2],
    'db_size_mb': round(os.path.getsize('$VPS_DB')/1024/1024, 1),
    'source': 'cachyos-laptop'
}
with open('$SUMMARY', 'w') as f:
    json.dump(result, f, indent=2)
print(f'📄 Summary saved ({result[\"db_size_mb\"]} MB)')
"

echo "✅ Sync complete!"
