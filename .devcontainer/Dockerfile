# [Choice] Python version (use -bullseye variants on local arm64/Apple Silicon): 3, 3.10, 3.9, 3.8, 3.7, 3.6, 3-bullseye, 3.10-bullseye, 3.9-bullseye, 3.8-bullseye, 3.7-bullseye, 3.6-bullseye, 3-buster, 3.10-buster, 3.9-buster, 3.8-buster, 3.7-buster, 3.6-buster
ARG VARIANT=3.10-bullseye
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

ENV PYTHONUNBUFFERED 1
ENV AIRFLOW_HOME=/workspaces/pipeline-environment/airflow_ais
ENV DAGSTER_HOME=/workspaces/pipeline-environment/dagster_ais

# [Choice] Node.js version: none, lts/*, 16, 14, 12, 10
ARG NODE_VERSION="none"
RUN if [ "${NODE_VERSION}" != "none" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

# Setup NATS client
ADD https://github.com/nats-io/natscli/releases/download/v0.0.35/nats-0.0.35-linux-amd64.zip nats.zip
RUN unzip nats.zip -d nats \
    && sudo cp nats/nats-0.0.35-linux-amd64/nats /usr/bin \
    && rm -r nats nats.zip

# [Optional] If your requirements rarely change, uncomment this section to add them to the image.
# COPY requirements.txt /tmp/pip-tmp/
# RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
#    && rm -rf /tmp/pip-tmp

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends postgresql-client vim

# Install dependencies
COPY .devcontainer/requirements.txt .
RUN pip install -r requirements.txt