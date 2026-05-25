# 📈 IDX Stock Updater

Auto-update 957 IDX stocks daily via **GitHub Actions** + **Yahoo Finance**.

## How it works

1. **GitHub Actions** (cron `0 9 * * 1-5`) runs `update_stocks.py`
2. Downloads current DB from VPS webhook → fetches latest OHLCV from Yahoo Finance
3. Uploads updated DB back to VPS via webhook  
4. Also commits to repo + stores as artifact

## Setup

### 1. GitHub Repo

```bash
# Create repo on GitHub, then:
git init
git add .
git commit -m "Initial"
git remote add origin https://github.com/YOUR_USER/idx-stock-updater.git
git push -u origin main
```

### 2. GitHub Secrets

Go to Settings → Secrets and variables → Actions → New repository secret:

| Secret | Value |
|---|---|
| `WEBHOOK_URL` | `http://YOUR_VPS_IP:8765` |
| `API_KEY` | `(your API key from webhook server)` |

### 3. Webhook Server (VPS)

```bash
# Generate a secure API key
export STOCK_API_KEY=$(openssl rand -hex 16)

# Start the server (add to systemd for persistence)
python3 ~/projects/stock-updater/webhook_server.py

# Or via nohup:
nohup python3 ~/projects/stock-updater/webhook_server.py > ~/stock_webhook.log 2>&1 &
```

### 4. Test

Trigger the workflow manually from GitHub Actions tab, or wait for the daily cron.

## Files

| File | Description |
|---|---|
| `update_stocks.py` | Main update script |
| `.github/workflows/update-stocks.yml` | GitHub Actions workflow |
| `webhook_server.py` | VPS-side receiver |
| `stocks.db` | SQLite database (auto-generated) |
