FROM ubuntu:16.04

LABEL description="BCH Ingestion script" \
      maintainer="mike@swatt.exchange" \
      version="0.1"

ENV HOME /root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        wget \
        curl \
        net-tools \
        vim

WORKDIR $HOME

COPY requirements.txt .
COPY ingest.py .
COPY python-bitcoin-blockchain-parser/ /root/python-bitcoin-blockchain-parser/

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
RUN pip3 install /root/python-bitcoin-blockchain-parser

ENTRYPOINT ["python3", "ingest.py"]
CMD ["bch", "/root/.bitcoin/blocks/", "10.99.125.147", "yellowbanana"]
