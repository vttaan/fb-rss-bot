#!/usr/bin/env bash
# ==============================================================================
# update.sh - Automated Git Pull & Service Restart for 24/7 Ngrok VM
# ==============================================================================
set -euo pipefail

# 1. Resolve project directory (script location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=================================================="
echo " [$(date '+%Y-%m-%d %H:%M:%S')] Starting VM Auto-Update"
echo " Working directory: $SCRIPT_DIR"
echo "=================================================="

# 2. Stash or rebase local runtime changes (e.g. auto-generated feeds/db) safely
echo ">> Stashing any uncommitted local changes..."
git stash || true

# 3. Pull latest commits from GitHub
echo ">> Pulling latest commits from origin/main..."
git fetch origin main
git pull --rebase origin main

# 4. Activate Python virtual environment
VENV_DIR=""
for venv_path in "$SCRIPT_DIR/venv" "$SCRIPT_DIR/.venv" "$HOME/venv" "$HOME/.venv"; do
    if [ -f "$venv_path/bin/activate" ]; then
        VENV_DIR="$venv_path"
        break
    fi
done

if [ -n "$VENV_DIR" ]; then
    echo ">> Activating virtual environment: $VENV_DIR"
    # shellcheck disable=SC1090
    source "$VENV_DIR/bin/activate"
else
    echo ">> [WARNING] Virtual environment not found. Using system python3."
fi

# 5. Upgrade Python dependencies if requirements.txt changed
if [ -f "requirements.txt" ]; then
    echo ">> Checking and installing updated dependencies..."
    pip install -q -r requirements.txt
fi

# 6. Ensure Playwright browser binaries are present
if command -v playwright &> /dev/null; then
    echo ">> Ensuring Playwright Chromium is installed..."
    playwright install chromium || true
fi

# 7. Restart the background server / scheduler daemon
echo ">> Restarting service daemon..."
if systemctl is-active --quiet rss-bot 2>/dev/null; then
    echo ">> Restarting systemd service: rss-bot"
    sudo systemctl restart rss-bot
elif systemctl is-active --quiet fb-rss 2>/dev/null; then
    echo ">> Restarting systemd service: fb-rss"
    sudo systemctl restart fb-rss
elif command -v pm2 &> /dev/null && pm2 list | grep -q "server"; then
    echo ">> Reloading PM2 process..."
    pm2 restart all
else
    echo ">> Checking for running server.py / main.py processes..."
    SERVER_PID=$(pgrep -f "python.*server\.py" || true)
    if [ -n "$SERVER_PID" ]; then
        echo ">> Stopping existing server (PID: $SERVER_PID)..."
        kill -15 "$SERVER_PID" 2>/dev/null || kill -9 "$SERVER_PID" 2>/dev/null
        sleep 2
    fi

    echo ">> Starting new server in background (nohup)..."
    nohup python server.py --host 0.0.0.0 --port 8000 >> "$SCRIPT_DIR/server.log" 2>&1 &
    sleep 2
    NEW_PID=$(pgrep -f "python.*server\.py" || true)
    echo ">> Server started with PID: $NEW_PID"
fi

echo "=================================================="
echo " [$(date '+%Y-%m-%d %H:%M:%S')] VM Update Completed Successfully"
echo "=================================================="
