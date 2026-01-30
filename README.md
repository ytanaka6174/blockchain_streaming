# blockchain_streaming

A pipeline for streaming Ethereum blockchain events into PostgreSQL via Kafka. Everything runs locally via Docker - useful for prototyping and experimenting with blockchain data before scaling up.

There are two ways to get blockchain data into the pipeline:
- **WebSocket subscriptions** - Connect directly to an RPC node via `eth_subscribe`
- **Webhooks** - Receive data from services like Alchemy or Infura

Events flow through Kafka, get processed by Spark Structured Streaming, and land in Postgres.

## Architecture

```
[Ethereum RPC / Webhooks] → [Kafka] → [Spark Streaming] → [PostgreSQL]
```

## Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
# Install dependencies
uv sync

# Start the infrastructure
docker compose up -d
```

The docker-compose spins up:
- Kafka (KRaft mode, no Zookeeper)
- PostgreSQL
- Spark (single node for local dev)
- ngrok (for webhook tunneling)

You'll need to set `NGROK_AUTHTOKEN` in your `.env` file if you want to use webhooks.

## Usage

### Option 1: WebSocket Subscription

The subscription approach connects directly to an RPC node. Check out [streams/subscriptions/ethena_example.py](streams/subscriptions/ethena_example.py) for an example that listens to Ethena contract events.

```bash
# Make sure Kafka is running first
uv run python -m streams.subscriptions.ethena_example
```

Most RPC nodes will require an API key for WebSocket connections. The example uses a public node but you'll probably want to swap that out.

### Option 2: Webhooks

If you'd rather use webhooks from a service like Alchemy:

```bash
# Start the Flask receiver
uv run python -m streams.webhooks.receiver_app
```

The ngrok container exposes a tunnel at `localhost:4040` where you can grab the public URL. Point your webhook provider at `<ngrok-url>/webhook`.

### Running the Spark Consumer

Once data is flowing into Kafka, start the Spark job to process it:

```bash
docker exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
    /opt/spark-apps/blockchain_consumer.py
```

This reads from the `transaction` topic, parses the event data, and writes to the `events` table in Postgres.

## Database Schema

The Spark job writes to a simple events table:

| Column | Type | Description |
|--------|------|-------------|
| contract_address | TEXT | Address that emitted the event |
| block_number | BIGINT | Block number (converted from hex) |
| transaction_hash | TEXT | Transaction hash |
| log_index | INT | Position in the block's logs |
| event_signature | TEXT | First topic (the event signature) |
| topics | TEXT | Comma-separated topic hashes |
| raw_data | TEXT | Event data payload |
| ingested_at | TIMESTAMP | When the record was written |

## Project Structure

```
├── kafka/
│   └── producer.py          # Simple Kafka producer wrapper
├── spark/
│   └── blockchain_consumer.py   # PySpark streaming job
├── streams/
│   ├── subscriptions/
│   │   ├── base.py          # Base class for eth_subscribe
│   │   └── ethena_example.py    # Example subscription
│   └── webhooks/
│       └── receiver_app.py  # Flask app for receiving webhooks
├── postgres/
│   └── create_schema.sql    # Table definitions
└── docker-compose.yml
```

## Next

- Decode event data in Spark using contract ABIs. Right now `raw_data` and `topics` are stored as hex strings - would be nice to parse them into actual values (addresses, amounts, etc.)

## Notes

- The Kafka setup uses KRaft mode (no Zookeeper dependency)
- Replication factor is set to 1 since this is for local development
- The Spark container is set to `sleep infinity` so you can submit jobs manually
- Checkpoints are stored in `./checkpoints` for stream recovery
