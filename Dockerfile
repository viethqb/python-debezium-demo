FROM python:3.10-slim

USER root

RUN apt update && apt install -y git openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install "git+https://github.com/viethqb/pydbzengine@main#egg=pydbzengine[dev,dlt,iceberg]"

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY src src
