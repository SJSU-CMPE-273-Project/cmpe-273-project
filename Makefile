.PHONY: up up-obs down reset ps logs topics kafka-tail psql proto-check

up:            ## start the core platform
	docker compose up -d

up-obs:        ## start platform + Jaeger tracing
	docker compose --profile obs up -d

down:          ## stop (keeps data)
	docker compose --profile obs down

reset:         ## stop and WIPE all volumes
	docker compose --profile obs down -v

ps:
	docker compose ps -a

logs:          ## follow logs, e.g. make logs s=kafka
	docker compose logs -f $(s)

topics:        ## list Kafka topics
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

kafka-tail:    ## tail notifications.requested with keys
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:9092 --topic notifications.requested \
	  --from-beginning --property print.key=true

psql:          ## open a SQL shell on the RelayGuard database
	docker compose exec postgres psql -U relayguard -d relayguard

proto-check:   ## compile the contract to make sure it's valid
	python3 -m grpc_tools.protoc -I contracts/proto --descriptor_set_out=/dev/null \
	  contracts/proto/relayguard/v1/notification.proto && echo "proto OK"
