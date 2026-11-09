FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- test (docker build --target test .) ---------------------------------
FROM base AS test
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY . .
CMD ["python", "-m", "pytest", "-q"]

# ---- runtime -------------------------------------------------------------
FROM base
COPY oracle ./oracle
# Non-root; nothing is written to disk (results are pulled via GET /report).
RUN useradd --system --uid 10001 app && chmod -R a+rX /app
USER app
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health').status==200 else 1)"
CMD ["uvicorn", "oracle.app:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log"]
