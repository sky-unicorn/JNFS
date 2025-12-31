#!/bin/bash
cd "$(dirname "$0")"

# Grant execute permission
chmod +x start-registry.sh
chmod +x start-namenode.sh
chmod +x start-datanode.sh

echo "Starting all JNFS components..."

# Start Registry in background
nohup ./start-registry.sh > registry.log 2>&1 &
echo "Registry started (pid $!), waiting 5s..."
sleep 5

# Start NameNode in background
nohup ./start-namenode.sh > namenode.log 2>&1 &
echo "NameNode started (pid $!), waiting 5s..."
sleep 5

# Start DataNode in background
nohup ./start-datanode.sh > datanode.log 2>&1 &
echo "DataNode started (pid $!)."

echo "All components started in background. Check logs in bin/*.log"
