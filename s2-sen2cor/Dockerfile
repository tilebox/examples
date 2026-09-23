FROM python:3.12-slim-bookworm

# ESA supplies the standalone processor for Linux x86_64 only.
RUN test "$(uname -m)" = x86_64 \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl file libexpat1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 worker
ENV HOME=/home/worker
RUN curl --fail --location --retry 3 \
      https://step.esa.int/thirdparties/sen2cor/2.12.0/Sen2Cor-02.12.04-Linux64.run \
      -o /tmp/sen2cor.run \
    && echo 'd6c1d16698cc1a493be100a0fde7217a7fd9a14f6c7a2fe373d98209511d5e3f  /tmp/sen2cor.run' | sha256sum -c - \
    && sh /tmp/sen2cor.run --quiet --target /opt/sen2cor \
    && rm /tmp/sen2cor.run \
    && chmod -R a+rX /opt/sen2cor \
    && chown -R worker:worker /home/worker
COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY sen2cor_workflow/ ./sen2cor_workflow/
COPY runner.py ./
RUN mkdir /work && chown worker:worker /work
ENV PATH="/app/.venv/bin:/opt/sen2cor/bin:$PATH" \
    WORK_DIR=/work \
    PYTHONUNBUFFERED=1
USER worker
RUN L2A_Process --help >/dev/null \
    && python -c 'import runner; import rasterio; from pathlib import Path; import os; assert os.getuid() == 10001; assert os.access("/work", os.W_OK); assert (Path.home() / "sen2cor/2.12/cfg/L2A_GIPP.xml").is_file()'
CMD ["python", "runner.py"]
