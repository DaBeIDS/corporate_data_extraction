FROM ubuntu:22.04
SHELL ["/bin/bash", "-c"]

# no prompt during installation:
ARG DEBIAN_FRONTEND=noninteractive

# Added poppler-utils, default-jre installations
RUN apt-get update && apt-get install -y apt-utils git wget python3 python3-pip vim ninja-build poppler-utils default-jre \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./esg_data_pipeline /app/code/esg_data_pipeline
COPY ./utils/s3_communication.py /app/code/esg_data_pipeline/esg_data_pipeline/s3_communication.py

RUN chgrp -R 0 /app/code && chmod g=u /app/code
RUN chmod -R 777 /app/code

WORKDIR /app/code/esg_data_pipeline

RUN pip install -e .

RUN mkdir -p /app/server_logs
RUN mkdir -p /app/data

RUN chmod -R 777 /app/server_logs
RUN chmod -R 777 /app/data

CMD ./entry.sh


