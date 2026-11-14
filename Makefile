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

.PHONY: contracts-generate contracts-build contracts-test iggy-up demo-java demo-python demo-ruby
contracts-generate: ## requires protoc 33.5 and protoc-gen-go 1.36.10
	tools/generate-contracts.sh

contracts-build:
	mkdir -p .state
	mvn -q -f services/core-banking-java/pom.xml package
	go build -o .state/iggy-smoke ./services/ingest-bridge-go/cmd/iggy-smoke
	docker build -t relayguard-ruby-contract -f services/auth-otp-ruby/Dockerfile .

contracts-test: contracts-build
	go test ./contracts/...
	python3 tools/test-sequencing.py
	PYTHONPATH=gen/python python3 tools/test-contracts.py

iggy-up:
	docker compose up -d iggy
	docker compose run --rm iggy-init

demo-java:
	python3 tools/demo-producer.py java --count 2

demo-python:
	python3 tools/demo-producer.py python --count 2

demo-ruby:
	python3 tools/demo-producer.py ruby --count 2

.PHONY: iggy-smoke
iggy-smoke: contracts-build
	python3 harness/iggy-smoke.py
