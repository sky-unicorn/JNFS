#!/bin/bash
cd "$(dirname "$0")/.."

JAR_PATH="jnfs-registry/target/jnfs-registry-0.0.1-SNAPSHOT.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo "Error: Cannot find $JAR_PATH"
    echo "Please run 'mvn package' first."
    exit 1
fi

echo "Starting JNFS Registry..."
java -jar "$JAR_PATH"
