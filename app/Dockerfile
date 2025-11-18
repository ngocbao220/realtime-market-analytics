FROM apache/spark:3.5.0

# Chuyển sang root để cài package
USER root

RUN pip install clickhouse-driver pandas

USER 1001
