# Ubuntu 16.04 with Java 8 installed.
# Build image with:  docker build -t flyem/neuprint .

FROM ubuntu:16.04

MAINTAINER Tom Dolafi, https://github.com/tomdolafi

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y openjdk-8-jre

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /app/data

WORKDIR /app

COPY neuprinter.jar .
