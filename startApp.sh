docker compose up -d

cd sourceConnection && curl -X POST -H "Content-Type: application/json"   --data @mysql-source.json   http://localhost:8083/connectors
cd ..

docker compose exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic flight-data-dlq \
  --partitions 1 \
  --replication-factor 1
