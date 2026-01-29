#!/usr/bin/env bash
set -euo pipefail

# Wait for Cassandra to be ready
echo "Waiting for Cassandra to be ready..."
until docker compose exec -T cassandra cqlsh -e "describe keyspaces" > /dev/null 2>&1; do
    echo "Cassandra not ready, waiting..."
    sleep 5
done

echo "Cassandra is ready. Creating schema..."

# Run the schema script
docker compose exec -T cassandra cqlsh < schemas/cassandra/init.cql

echo "Schema created successfully."

# Verify
echo ""
echo "Verifying tables:"
docker compose exec -T cassandra cqlsh -e "USE topk; DESCRIBE TABLES;"
