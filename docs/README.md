# Infrastructure quick start

This document contains quick operational commands to bring up the local infrastructure for the Finance Analytics demo, register the Debezium connector, and inspect the running services and UIs.

All commands assume you run them from the repository root unless an absolute path is shown.

## Start all services

Change into the infra folder and start the entire stack in detached mode:

```bash
cd /home/bnguyen/Desktop/finance_analytics/infra
sudo docker compose up -d
```

## Verify services are running

List running compose services and check any configured container healthchecks:

```bash
docker compose ps
```

## Register the Debezium connector

Register the MySQL connector by posting the connector configuration JSON to the Connect REST API. The example below references the connector config file in this repository:

```bash
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  "http://localhost:8083/connectors/" \
  -d @/home/bnguyen/Desktop/finance_analytics/infra/config/register-mysql.json
```

## Delete connector

Remove the connector if you need to tear it down or re-register:

```bash
curl -i -X DELETE "http://localhost:8083/connectors/finance-mysql-connector"
```

## Check connector status

Inspect the connector and task state with the status endpoint. Pipe to `jq` for pretty JSON output if installed:

```bash
curl -s "http://localhost:8083/connectors/finance-mysql-connector/status" | jq
```

## Check Kafka topics

List topics created for the pipeline to confirm Debezium is writing to Kafka:

```bash
sudo docker exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

## Access MySQL

Connect with a GUI or CLI. Example credentials and host used in this demo:

- MySQL Workbench: `localhost:30306`, user `root`, password `root123`
- From a shell inside the MySQL container:

```bash
sudo docker exec -it mysql mysql -u root -p
```

## Monitoring UIs

Open the following web UIs in your browser to inspect and manage the platform:

- Kafka UI: http://localhost:9089
- Spark UI: http://localhost:4040
- MinIO Console: http://localhost:9901
- Airflow UI: http://localhost:8082
- Streamlit dashboard: http://localhost:8501
- Dremio UI: http://localhost:9047
- Superset UI: http://localhost:8080

## Notes & tips

- If any container fails to start or reports unhealthy, tail its logs for diagnostics: `docker logs -f <container_name>`.
- Adjust Docker resources (memory/CPU) if Spark or other services run out of memory.
- The connector configuration file referenced above is `infra/config/register-mysql.json` — edit that file to change connector settings before registering.
