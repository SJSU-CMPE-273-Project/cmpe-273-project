# Build from the repo root's compose file (it passes contracts/ as a second build context):
#   docker compose build load-generator
# or by hand:
#   docker build --build-context contracts=contracts -t loadgen harness/load-generator

# ---- generate Python classes from the shared contract -----------------------
FROM python:3.12-slim AS proto
RUN pip install --no-cache-dir grpcio-tools==1.68.1
COPY --from=contracts proto /contracts/proto
RUN mkdir /gen && python -m grpc_tools.protoc -I /contracts/proto --python_out=/gen \
      /contracts/proto/relayguard/v1/notification.proto \
 && touch /gen/relayguard/__init__.py /gen/relayguard/v1/__init__.py

FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/gen
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=proto /gen /gen

# ---- test (docker build --build-context contracts=contracts --target test harness/load-generator)
FROM base AS test
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY . .
CMD ["python", "-m", "pytest", "-q"]

# ---- runtime ------------------------------------------------------------------
FROM base
COPY loadgen ./loadgen
COPY demo_trigger.py ./demo_trigger.py
# /runs holds event files for replay (T3/T4/T5). Owned by the app user so a
# named volume mounted there starts out writable.
RUN useradd --system --uid 10001 app && mkdir /runs && chown app /runs
USER app
ENTRYPOINT ["python", "-m", "loadgen"]
