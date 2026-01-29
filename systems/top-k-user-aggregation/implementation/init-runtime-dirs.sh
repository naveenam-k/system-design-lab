#!/usr/bin/env bash
set -euo pipefail

# Create runtime directories for Docker bind mounts
RUNTIME_BASE="/runtime/shared/system-design-lab/top_k_user_aggregation"

echo "Creating runtime directories at $RUNTIME_BASE ..."

sudo mkdir -p "$RUNTIME_BASE/zookeeper/data"
sudo mkdir -p "$RUNTIME_BASE/zookeeper/log"
sudo mkdir -p "$RUNTIME_BASE/kafka"
sudo mkdir -p "$RUNTIME_BASE/postgres"
sudo mkdir -p "$RUNTIME_BASE/cassandra"
sudo mkdir -p "$RUNTIME_BASE/redis"

# Set permissions (Docker containers often run as specific users)
sudo chmod -R 777 "$RUNTIME_BASE"

echo "Runtime directories created:"
ls -la "$RUNTIME_BASE"
