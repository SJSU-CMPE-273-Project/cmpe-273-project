# cmpe-273-project
CMPE 273 team project — SJSU, Fall 2026

## Project ideas

Three proposals for review. Each targets a different failure mode and maps
to course modules. Stack in all three: Flask, Postgres, Kafka, Docker.

### 1. Durable agent execution over a shared log

Agents crash mid-task and redo work they already completed — a booking
agent books a flight, dies, restarts, and books a second one. We log each
step to a shared log before executing it, so a restarted worker resumes
where it left off instead of replaying side effects.

Demo: kill the worker mid-run and show the task completing exactly once.
Modules: fault tolerance, event-driven systems.

### 2. Reliable messaging service with retry semantics

A messaging service that handles send failures: retries with backoff,
dead-letter handling for messages that never succeed, and de-duplication
so retries don't deliver twice.

Demo: drop the downstream consumer, show queued retries and the
dead-letter path, then bring it back and show recovery.
Modules: communication, fault tolerance.

### 3. Tool-call authorization proxy for agents

Agents call tools (db writes, payments, email) using the service's own
credentials, so a prompt-injected agent can do anything the service can.
A proxy in front of every tool call checks whether this agent, acting for
this user, may perform this action, and propagates the original user
identity through the chain instead of a shared service account.

Demo: feed the agent a poisoned input, watch the proxy block the
unauthorized tool call while the audit log shows who asked for what.
Modules: security in distributed and agentic systems.
