FROM postgres:16

RUN apt-get update && apt-get install -y \
    postgresql-plpython3-16 \
    python3-pip \
    && pip3 install eth_abi --break-system-packages \
    && rm -rf /var/lib/apt/lists/*
