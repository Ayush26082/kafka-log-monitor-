up:        ; docker compose up -d --build
down:      ; docker compose down -v
logs:      ; docker compose logs -f log-processor alert-notifier
scale:     ; docker compose up -d --scale log-processor=3
groups:    ; docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group log-processor
dlq:       ; docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic app-logs.dlq --from-beginning --timeout-ms 5000
test:      ; pytest -q
