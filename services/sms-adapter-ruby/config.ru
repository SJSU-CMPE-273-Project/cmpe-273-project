$stdout.sync = true # flush log lines immediately so `docker compose logs` shows them live
require_relative "lib/telemetry"
Telemetry.setup! # no-op unless OTEL_EXPORTER_OTLP_ENDPOINT is set
require_relative "app"
run SmsAdapter
