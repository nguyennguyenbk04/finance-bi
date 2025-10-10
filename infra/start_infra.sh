#!/bin/bash

# Load environment variables from .env file
if [ -f "$(dirname "$0")/.env" ]; then
    export $(grep -v '^#' "$(dirname "$0")/.env" | xargs)
fi

# Set Airflow UID for proper permissions (if using volume mounts)
export AIRFLOW_UID=$(id -u)

sudo docker compose up -d

sleep 15

# Register MySQL source connector
CONFIG_FILE="$(dirname "$0")/config/register-mysql.json"
if [ ! -f "$CONFIG_FILE" ]; then
    exit 1
fi

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
     localhost:8083/connectors/ \
     -d @"$CONFIG_FILE"

sleep 5

curl -s localhost:8083/connectors/finance-mysql-connector/status | jq

echo "Initializing Kafka topics..."
INIT_SCRIPT="$(dirname "$0")/database/scripts/initialize_topics.sql"

if [ -f "$INIT_SCRIPT" ]; then
    docker exec -i mysql mysql -u${MYSQL_USER:-root} -p${MYSQL_ROOT_PASSWORD:-rootpassword} < "$INIT_SCRIPT"
    sleep 15
    echo "Kafka topics initialized successfully"
else
    echo "Warning: initialize_topics.sql not found at $INIT_SCRIPT"
fi

