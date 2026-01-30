"""
    This is an example kafka producer that sends events from an eth_subscription
    As an example, a public node is used here that subscribed to events emitted from Ethena contracts
"""

import asyncio
import json
from streams.subscriptions.base import EthSubscription
from kafka.producer import send_message

# Ethena contract addresses on Ethereum mainnet
USDE_ADDRESS = "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"
SUSDE_ADDRESS = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
ENA_ADDRESS = "0x57e114B691Db790C35207b2e685D4A43181e6061"

# Common event topic signatures
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# URI - doesn't require an api-key but most nodes will
example_uri = "wss://ethereum-rpc.publicnode.com"

def handle_ethena_event(log_data):
    print(f"\n--- Ethena Event ---")
    print(f"Block: {log_data['blockNumber']}")
    print(f"Tx: {log_data['transactionHash']}")
    print(f"Contract: {log_data['address']}")
    print(f"Topics: {log_data['topics']}")
    print(f"Data: {log_data['data']}")

    send_message(topic='transaction', value=json.dumps(log_data).encode('utf-8'))

async def main():
    # Use a WebSocket RPC endpoint (publicnode provides wss)
    sub = EthSubscription(example_uri)

    # Subscribe to logs from Ethena contracts (USDe, sUSDe, ENA)
    await sub.subscribe_to_logs(
        address=[USDE_ADDRESS, SUSDE_ADDRESS, ENA_ADDRESS],
        topics=[TRANSFER_TOPIC],  # Filter for Transfer events
        on_event=handle_ethena_event
    )

if __name__ == "__main__":
    asyncio.run(main())