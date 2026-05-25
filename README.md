# 📈 IDX Stock Updater

Auto-update 957 IDX stocks daily.

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐
│  Advan Tab (CachyOS)│     │  VPS (Oracle Cloud)  │
│                     │     │                      │
│  Cron 17:15 WITA    │     │  Cron 17:10 WITA     │
│  └→ yfinance update │     │  └→ yfinance update  │
│  └→ saves stocks.db │     │     (if rate ok)     │
│                     │     │                      │
│                     │     │  Cron 17:30 WITA     │
│                     │     │  └→ sync from laptop │
│                     │     │     (via Tailscale)  │
└─────────────────────┘     └──────────────────────┘
                                     │
                                     ▼
                            ~/.hermes/databases/stocks.db
```

## VPS Setup

```bash
# Cron sudah terdaftar:
# 10 9 * * 1-5  → VPS direct update (yfinance)
# 30 9 * * 1-5  → Sync from laptop (Tailscale fallback)

# Cek log update:
tail -f ~/projects/stock-updater/update.log

# Cek DB status:
python3 -c "import sqlite3; conn=sqlite3.connect('$HOME/.hermes/databases/stocks.db'); r=conn.execute('SELECT MAX(date),COUNT(DISTINCT symbol),COUNT(*) FROM stock_ohlcv').fetchone(); conn.close(); print(f'Latest: {r[0]}, Stocks: {r[1]}, Rows: {r[2]:,}')"
```

## Files

| File | Description |
|------|-------------|
| `update_stocks_vps.py` | Main update script (VPS) |
| `sync_from_laptop.sh` | Sync from CachyOS via Tailscale |
| `stocks.db` | SQLite database (107MB, 957 stocks) |
| `updated_stocks.json` | Summary of latest update |
