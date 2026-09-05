ARG SUPERSET_IMAGE=apache/superset:6.0.0
FROM ${SUPERSET_IMAGE}

USER root
COPY infra/docker/superset/requirements.txt /requirements.txt
RUN uv pip install -p /app/.venv/bin/python --no-cache -r /requirements.txt

USER superset
