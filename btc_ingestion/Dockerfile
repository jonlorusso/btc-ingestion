FROM ubuntu:16.04

LABEL description="BTC Ingestion script" \
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

RUN pip3 install -r requirements.txt
RUN python3 /root/python-bitcoin-blockchain-parser/setup.py install

ENTRYPOINT ["python3", "ingest.py"]
CMD ["btc", "/mnt/BTC_3", "10.99.125.147", "yellowbanana"]
