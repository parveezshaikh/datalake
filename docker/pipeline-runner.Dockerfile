# syntax=docker/dockerfile:1.4

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/opt/app \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8080

WORKDIR ${APP_HOME}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="${JAVA_HOME}/bin:${PATH}" \
    PYTHONPATH=${APP_HOME}

COPY pyproject.toml ./pyproject.toml
COPY libs ./libs
COPY services ./services
COPY config ./config
COPY data ./data

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pyspark>=3.5.0 \
        fastapi>=0.110.0 \
        uvicorn>=0.27.0 \
        pydantic>=2.6.0 \
        python-dotenv>=1.0.0 \
        pandas>=2.2.0 \
        numpy>=1.26.0 \
        prometheus-client>=0.19.0 \
        requests>=2.31.0

EXPOSE ${UVICORN_PORT}

ENTRYPOINT ["uvicorn", "services.orchestrator.main:app"]
CMD ["--host", "0.0.0.0", "--port", "8080"]
