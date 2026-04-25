FROM apache/airflow:2.8.1-python3.10

# Install Docker CLI so Airflow can run: docker exec hive-server hive -e "..."
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && usermod -aG docker airflow

USER airflow
RUN pip install --no-cache-dir \
    requests==2.31.0 \
    confluent-kafka==2.3.0 \
    hdfs==2.7.3 \
    pandas==2.1.4 \
    pyarrow==14.0.2
