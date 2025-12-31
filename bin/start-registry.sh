#!/bin/bash
cd "$(dirname "$0")/.."

JAR_NAME="JNFS-0.0.1-SNAPSHOT.jar"
TARGET_DIR="target"

if [ -f "$TARGET_DIR/$JAR_NAME" ]; then
    JAR_PATH="$TARGET_DIR/$JAR_NAME"
elif [ -f "$JAR_NAME" ]; then
    JAR_PATH="$JAR_NAME"
else
    echo "Error: Cannot find $JAR_NAME"
    exit 1
fi

echo "Starting JNFS Registry..."
java -cp "$JAR_PATH" org.jnfs.registry.RegistryServer
