#!/bin/bash

# Get script absolute path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_HOME="$(dirname "$DIR")"
CONF_DIR="$APP_HOME/conf"
LIB_DIR="$APP_HOME/lib"
PID_DIR="$APP_HOME/pids"

SERVICE=$1

# 等待 Registry RPC 端口就绪。Registry 首次启动需初始化 H2 文件库，在 Linux 上可能耗时十几秒，
# 若不等待，NameNode/DataNode 会在 Registry 尚未监听时连接失败。
wait_for_registry() {
    local port="$1"
    local timeout=60
    local waited=0
    echo "Waiting for Registry (port $port)..."
    while [ $waited -lt $timeout ]; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            echo "Registry is ready."
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "WARNING: Registry (port $port) not ready after ${timeout}s, continuing anyway."
    return 1
}

if [ -z "$SERVICE" ]; then
    echo "Starting all services..."
    "$DIR/start.sh" registry
    # 从 conf/registry.yml 解析 Registry RPC 端口（默认 5367），等待就绪后再启动 NameNode/DataNode
    REGISTRY_PORT=$(awk '/^server:/{found=1} found && /port:/{gsub(/[^0-9]/,""); print; exit}' "$CONF_DIR/registry.yml" 2>/dev/null)
    REGISTRY_PORT=${REGISTRY_PORT:-5367}
    wait_for_registry "$REGISTRY_PORT"
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
