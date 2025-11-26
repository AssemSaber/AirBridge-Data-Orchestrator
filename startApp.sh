docker compose up -d
cd sourceConnection && curl -X POST -H "Content-Type: application/json"   --data @mysql-source.json   http://localhost:8083/connectors
cd ..

#  start snowflake sink
# curl -X POST -H "Content-Type: application/json" \
#   --data @snowflake-sink.json \
#   http://localhost:8083/connectors