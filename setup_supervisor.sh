#!/bin/bash
# setup_supervisor.sh - Setup HedgeFund Agent with Supervisor

set -e

echo "🚀 Setting up HedgeFund Agent with Supervisor"

# Check if running as correct user
if [ "$USER" != "hunter" ]; then
    echo "❌ Please run this script as the 'hunter' user"
    exit 1
fi

# Variables
PROJECT_DIR="/home/hunter/projects/hedgefund_agent"
VENV_DIR="/home/hunter/projects/database/venv"  # Using shared venv
LOGS_DIR="$PROJECT_DIR/logs"
SUPERVISOR_CONF="/etc/supervisor/conf.d/hedgefund-scheduler.conf"

echo "📁 Project directory: $PROJECT_DIR"

# Create logs directory
echo "📁 Creating logs directory..."
mkdir -p "$LOGS_DIR"
chmod 755 "$LOGS_DIR"

# Verify virtual environment
echo "🐍 Checking virtual environment..."
if [ ! -f "$VENV_DIR/bin/python" ]; then
    echo "❌ Virtual environment not found at $VENV_DIR"
    echo "💡 Create it with: python3 -m venv venv"
    exit 1
fi

# Test scheduler can import (basic validation)
echo "🧪 Testing scheduler imports..."
cd "$PROJECT_DIR"
if ! "$VENV_DIR/bin/python" -c "from scheduler import HedgeFundScheduler; print('✅ Imports OK')"; then
    echo "❌ Scheduler import failed. Check dependencies."
    exit 1
fi

# Install supervisor if not present
if ! command -v supervisord &> /dev/null; then
    echo "📦 Installing supervisor..."
    sudo apt update
    sudo apt install -y supervisor
else
    echo "✅ Supervisor already installed"
fi

# Create supervisor config
echo "⚙️ Creating supervisor configuration..."
sudo tee "$SUPERVISOR_CONF" > /dev/null << 'EOF'
[program:hedgefund-scheduler]
command=/home/hunter/projects/database/venv/bin/python /home/hunter/projects/hedgefund_agent/scheduler.py
directory=/home/hunter/projects/hedgefund_agent
user=hunter
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/home/hunter/projects/hedgefund_agent/logs/scheduler.log
stdout_logfile_maxbytes=10MB
stdout_logfile_backups=5
environment=PATH="/home/hunter/projects/database/venv/bin:%(ENV_PATH)s"
startsecs=10
stopwaitsecs=30
killasgroup=true
priority=100

[group:hedgefund-agent]
programs=hedgefund-scheduler
priority=100
EOF

echo "✅ Supervisor config created at $SUPERVISOR_CONF"

# Reload supervisor configuration
echo "🔄 Reloading supervisor configuration..."
sudo supervisorctl reread
sudo supervisorctl update

# Check if service exists and start it
echo "🚀 Starting HedgeFund scheduler..."
if sudo supervisorctl status hedgefund-scheduler &> /dev/null; then
    sudo supervisorctl restart hedgefund-scheduler
else
    sudo supervisorctl start hedgefund-scheduler
fi

# Wait a moment for startup
sleep 3

# Check status
echo "📊 Checking scheduler status..."
STATUS=$(sudo supervisorctl status hedgefund-scheduler)
echo "$STATUS"

if echo "$STATUS" | grep -q "RUNNING"; then
    echo "✅ HedgeFund scheduler is running!"
    echo "📋 View logs: tail -f $LOGS_DIR/scheduler.log"
    echo "🎛️ Control via: sudo supervisorctl {start|stop|restart|status} hedgefund-scheduler"
else
    echo "❌ Scheduler failed to start. Check logs:"
    echo "📄 Supervisor logs: sudo tail /var/log/supervisor/supervisord.log"
    echo "📄 Scheduler logs: tail $LOGS_DIR/scheduler.log"
    exit 1
fi

echo ""
echo "🎉 Setup complete! HedgeFund Agent is now running under supervisor."
echo ""
echo "📚 Useful commands:"
echo "  • Status:  sudo supervisorctl status hedgefund-scheduler"
echo "  • Restart: sudo supervisorctl restart hedgefund-scheduler"
echo "  • Stop:    sudo supervisorctl stop hedgefund-scheduler"
echo "  • Logs:    tail -f $LOGS_DIR/scheduler.log"
echo "  • Errors:  grep ERROR $LOGS_DIR/scheduler.log"