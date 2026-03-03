FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends     default-jre-headless     ca-certificates  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir jpype1

COPY server.py /app/server.py
COPY drivers/ /app/drivers/

ENV VERTICA_JDBC_JAR=/app/drivers/vertica-jdbc.jar

CMD ["python", "/app/server.py"]
