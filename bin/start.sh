#!/bin/bash

# 获取脚本绝对路径
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_HOME="$(dirname "$DIR")"
CONF_DIR="$APP_HOME/conf"
LIB_DIR="$APP_HOME/lib"

SERVICE=$1

if [ -z "$SERVICE" ]; then
    echo "Usage: ./start.sh [registry|namenode|datanode|example]"
    exit 1
fi

if [ "$SERVICE" = "registry" ]; then
    MAIN_CLASS="org.jnfs.registry.RegistryServer"
elif [ "$SERVICE" = "namenode" ]; then
    MAIN_CLASS="org.jnfs.namenode.NameNodeServer"
elif [ "$SERVICE" = "datanode" ]; then
    MAIN_CLASS="org.jnfs.datanode.DataNodeServer"
elif [ "$SERVICE" = "example" ]; then
    MAIN_CLASS="org.jnfs.example.ExampleApp"
else
    echo "Unknown service: $SERVICE"
    exit 1
fi

echo "Starting $SERVICE..."
echo "APP_HOME: $APP_HOME"

# 检查 lib 目录是否存在
if [ ! -d "$LIB_DIR" ]; then
    echo "Error: lib directory not found at $LIB_DIR"
    exit 1
fi

# 运行 Java 程序
# 注意: Linux 下 classpath 分隔符是 :
java -DAPP_HOME="$APP_HOME" -Dlogback.configurationFile="$CONF_DIR/logback-${SERVICE}.xml" -cp "$CONF_DIR:$LIB_DIR/*" "$MAIN_CLASS"
