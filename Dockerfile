FROM bitnami/spark:latest

USER root

RUN pip install pandas psycopg2-binary pyspark

COPY etl_script.py /opt/spark-apps/

RUN mkdir -p /opt/bitnami/spark/.ivy2 && \
    chown -R 1001:1001 /opt/bitnami/spark/.ivy2 && \
    chmod 777 /opt/bitnami/spark/.ivy2

ENTRYPOINT ["spark-submit", "/opt/spark-apps/etl_script.py"]