sudo docker compose up -d
sleep 5

docker ps
sleep 5

# Register MySQL source connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json"\
 localhost:8083/connectors/ \
  -d @/home/bnguyen/Desktop/finance_analytics/infra/config/register-mysql.json
sleep 5


# Check connector status
curl -s localhost:8083/connectors/finance-mysql-connector/status | jq
