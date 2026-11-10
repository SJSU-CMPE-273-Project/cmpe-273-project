# Who owns what

Fill in names, then each person works in their own folder and opens PRs.

| Component | Folder | Language | Owner | Tests owned |
|---|---|---|---|---|
| Core Banking producer | `services/core-banking-java` | Java | | T3, T4 |
| Fraud Engine producer | `services/fraud-engine-python` | Python | | T7 |
| Auth / OTP producer | `services/auth-otp-ruby` | Ruby | | OTP TTL |
| Ingest Bridge | `services/ingest-bridge-go` | Go | | T3, bridge crash |
| Delivery Worker | `services/delivery-worker-go` | Go | Aaron (Lane 2) | T2, T4, T5, T6 |
| SMS Adapter (×2) | `services/sms-adapter-ruby` | Ruby | | T1, T6 |
| Push / Email Adapter | `services/push-email-adapter-node` | Node.js | | fallback |
| Ops Console | `services/ops-console-react` | TypeScript | | DLQ replay |
| Harness (load, chaos, oracle) | `harness/` | any | | runs T1–T7 |
| Platform / compose / AWS | `infra/`, `docker-compose.yml` | — | | AWS deploy |
| Contract | `contracts/` | Protobuf | **whole team** | — |

## Milestones to the Nov 30 review
| Week of | Goal | Done when |
|---|---|---|
| Oct 26 | Platform up, contract agreed | `make up` works on everyone's laptop; proto reviewed |
| Nov 2 | Happy path | 1 event: producer → Iggy → bridge → Kafka → worker → adapter → oracle |
| Nov 9 | Retries, failover, DLQ, replay; tracing | T1, T5, T6 pass by hand |
| Nov 16 | Harness + guardrails; security (Module 11) | T1–T7 run from a script with a report |
| Nov 23 | Deploy to AWS (RDS); rehearse failure demo | Live run on AWS |
| Nov 30 | **Architecture & failure-testing review** | |
| Dec 7 | **Final demo** | |
