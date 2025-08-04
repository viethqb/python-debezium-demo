FROM python:3.10-slim

USER root

RUN apt update
RUN apt install -y \
    gcc \
    g++ \
    unixodbc-dev \
    curl \
    gnupg \
    git \
    openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1

# get mssql source list
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg && \
    curl -sSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql.list

RUN apt update
RUN ACCEPT_EULA='Y' apt install -y msodbcsql18

RUN rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install pyodbc
RUN pip install "git+https://github.com/viethqb/pydbzengine@main#egg=pydbzengine[dev,dlt,iceberg]"

WORKDIR /app

COPY src src
