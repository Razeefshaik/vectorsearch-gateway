FROM python:3.13-slim

# Shared, writable cache dir for the embedding model -- downloaded once at
# build time (below) so containers never depend on network access to
# Hugging Face at startup or restart.
ENV HF_HOME=/app/.cache/huggingface \
    TRANSFORMERS_CACHE=/app/.cache/huggingface

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

RUN useradd -u 10001 -M -s /usr/sbin/nologin appuser && chown -R appuser:appuser /app
USER appuser

# gRPC port is passed as the command arg (see docker-compose.yml); the
# metrics/health port comes from METRICS_PORT (default 9100) so this
# HEALTHCHECK and the server agree on it without duplicating the value.
EXPOSE 50051 9100

HEALTHCHECK --interval=10s --timeout=5s --start-period=180s --retries=12 \
    CMD python3 -c "import os,urllib.request; urllib.request.urlopen('http://localhost:' + os.environ.get('METRICS_PORT','9100') + '/healthz', timeout=3)" || exit 1
ENTRYPOINT ["python", "-u", "server.py"]
