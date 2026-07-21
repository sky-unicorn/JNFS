#!/bin/bash

# Get script absolute path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_HOME="$(dirname "$DIR")"
CONF_DIR="$APP_HOME/conf"
LIB_DIR="$APP_HOME/lib"
PID_DIR="$APP_HOME/pids"

SERVICE=$1

if [ -z "$SERVICE" ]; then
    echo "Starting all services..."
    "$DIR/start.sh" registry
    "$DIR/start.sh" namenode
    "$DIR/start.sh" datanode
    exit 0
fi

if [ "$SERVICE" = "registry" ]; then
    MAIN_CLASS="org.jnfs.registry.RegistryServer"
elif [ "$SERVICE" = "namenode" ]; then
    MAIN_CLASS="org.jnfs.namenode.NameNodeServer"
elif [ "$SERVICE" = "datanode" ]; then
    MAIN_CLASS="org.jnfs.datanode.DataNodeServer"
else
    echo "Unknown service: $SERVICE"
    echo "Usage: $0 [registry|namenode|datanode]"
    exit 1
fi

echo "Starting $SERVICE..."
echo "APP_HOME: $APP_HOME"

# Check if lib directory exists
if [ ! -d "$LIB_DIR" ]; then
    echo "Error: lib directory not found at $LIB_DIR"
    exit 1
fi

# Create pids directory
mkdir -p "$PID_DIR"
PID_FILE="$PID_DIR/$SERVICE.pid"

# Check if already running
if [ -f "$PID_FILE" ]; then
    EXISTING_PID=$(cat "$PID_FILE")
    if kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "Error: $SERVICE is already running with PID $EXISTING_PID"
        exit 1
    else
        echo "Warning: stale PID file detected, removing $PID_FILE"
        rm -f "$PID_FILE"
    fi
fi

# Run Java program
# Note: Linux classpath separator is :
# Pin cwd to APP_HOME so relative data paths (namenode_meta.log, node_id.dat) resolve there
cd "$APP_HOME"
nohup java -DAPP_HOME="$APP_HOME" -Dlogback.configurationFile="$CONF_DIR/logback-${SERVICE}.xml" -cp "$CONF_DIR:$LIB_DIR/*" "$MAIN_CLASS" > /dev/null 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"
echo "$SERVICE started with PID $PID"
