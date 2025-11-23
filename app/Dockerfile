FROM apache/spark:3.5.0

# Chuyển sang root để cài package
USER root

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

USER 1001
