'use strict';

// Tracing must be set up before node:http is loaded (the instrumentation
// patches it on load), so this runs before requiring the app.
require('./src/telemetry').setup(); // no-op unless OTEL_EXPORTER_OTLP_ENDPOINT is set

const http = require('node:http');
const { createApp } = require('./src/app');

const port = Number.parseInt(process.env.PORT || '8080', 10);
const server = http.createServer(createApp());
// Drop idle keep-alive sockets before a proxy (Toxiproxy) would, to avoid
// sending a request on a socket the other side is closing.
server.keepAliveTimeout = 5000;

server.listen(port, '0.0.0.0', () => {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), svc: 'push-email-adapter', event: 'listening', port })}\n`);
});

// Stop accepting new requests and let in-flight ones finish when the container
// stops. (With tracing on, telemetry.js also flushes spans on SIGTERM.)
process.once('SIGTERM', () => server.close());
