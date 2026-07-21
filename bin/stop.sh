#!/bin/bash

# Get script absolute path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_HOME="$(dirname "$DIR")"
PID_DIR="$APP_HOME/pids"

SERVICE=$1

if [ -z "$SERVICE" ]; then
    echo "Stopping all services..."
    "$DIR/stop.sh" datanode
    "$DIR/stop.sh" namenode
    "$DIR/stop.sh" registry
    exit 0
fi

if [ "$SERVICE" != "registry" ] && [ "$SERVICE" != "namenode" ] && [ "$SERVICE" != "datanode" ]; then
    echo "Unknown service: $SERVICE"
    echo "Usage: $0 [registry|namenode|datanode]"
    exit 1
fi

PID_FILE="$PID_DIR/$SERVICE.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "$SERVICE is not running (no PID file at $PID_FILE)."
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
    echo "$SERVICE is not running (stale PID $PID, cleaning up)."
    rm -f "$PID_FILE"
    exit 0
fi

echo "Stopping $SERVICE (PID $PID)..."
# Send SIGTERM to trigger JVM shutdown hook (graceful shutdown)
kill "$PID"

# Wait up to 30 seconds for graceful shutdown
WAIT=30
for i in $(seq 1 "$WAIT"); do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "$SERVICE stopped gracefully."
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# Force kill if still running
echo "Warning: $SERVICE did not stop within ${WAIT}s, sending SIGKILL..."
kill -9 "$PID" 2>/dev/null
rm -f "$PID_FILE"
echo "$SERVICE killed."
