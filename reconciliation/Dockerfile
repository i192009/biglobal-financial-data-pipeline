FROM python:3.12-slim
WORKDIR /app
COPY reconciliation/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY reconciliation /app/reconciliation
COPY scripts /app/scripts
COPY dbt /app/dbt
COPY tests /app/tests
COPY pyproject.toml /app/pyproject.toml
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app RUFF_CACHE_DIR=/tmp/ruff-cache PYTEST_ADDOPTS="-p no:cacheprovider"
USER 65532:65532
CMD ["python", "-m", "reconciliation.service"]
