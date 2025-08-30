FROM mcr.microsoft.com/devcontainers/python:3.11

# Install system dependencies
RUN apt-get update && apt-get install -y \
    mysql-client \
    postgresql-client \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Spark (for local testing)
RUN curl -O https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar xf spark-3.5.0-bin-hadoop3.tgz \
    && mv spark-3.5.0-bin-hadoop3 /opt/spark \
    && rm spark-3.5.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Databricks CLI
RUN pip install databricks-cli

# Create workspace directory
WORKDIR /workspace

# Switch to non-root user
USER vscode