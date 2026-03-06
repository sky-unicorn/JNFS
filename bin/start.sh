#!/bin/bash

# Get script absolute path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_HOME="$(dirname "$DIR")"
CONF_DIR="$APP_HOME/conf"
LIB_DIR="$APP_HOME/lib"

SERVICE=$1

if [ -z "$SERVICE" ]; then
    echo "Starting all services..."
    "$0" registry
    "$0" namenode
    "$0" datanode
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

# Run Java program
# Note: Linux classpath separator is :
nohup java -DAPP_HOME="$APP_HOME" -Dlogback.configurationFile="$CONF_DIR/logback-${SERVICE}.xml" -cp "$CONF_DIR:$LIB_DIR/*" "$MAIN_CLASS" > /dev/null 2>&1 &
echo "$SERVICE started with PID $!"
