$stdout.sync = true # flush log lines immediately so `docker compose logs` shows them live
require_relative "app"
run SmsAdapter
