#!/bin/bash

echo "=== ksqlDB Admin Tools ==="
echo ""
echo "1. Show all streams"
echo "2. Show all tables"
echo "3. Describe stream"
echo "4. Show running queries"
echo "5. Server info"
echo "6. Exit"
echo ""
read -p "Select option: " option

case $option in
  1)
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d '{"ksql": "SHOW STREAMS;"}' | python -m json.tool
    ;;
  2)
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d '{"ksql": "SHOW TABLES;"}' | python -m json.tool
    ;;
  3)
    read -p "Stream name: " stream_name
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d "{\"ksql\": \"DESCRIBE ${stream_name} EXTENDED;\"}" | python -m json.tool
    ;;
  4)
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d '{"ksql": "SHOW QUERIES;"}' | python -m json.tool
    ;;
  5)
    curl http://localhost:8088/info | python -m json.tool
    ;;
  *)
    exit 0
    ;;
esac
