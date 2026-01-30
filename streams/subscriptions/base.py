"""
    This is the base class for creating a subscription via eth_subscribe on available rpc nodes that allow websockets
    Right now it only has subscribe_to_logs but you can subscribe to other things too like pending transactions
""" 
import asyncio
import json
import websockets

class EthSubscription:

    def __init__(self, uri, api_key=None):
        self.uri = uri
        self.api_key = api_key
        self.subscription_id = None
        self._ws = None
        self._running = False

    async def subscribe_to_logs(self, address=None, topics=None, on_event=None):
        """
        Subscribe to Ethereum logs via WebSocket.

        Args:
            address: Optional address or list of addresses to filter
            topics: Optional list of topic hashes to filter events
            on_event: Optional callback function that receives log_data dict
        """
        log_filter = {}
        if address:
            log_filter["address"] = address
        if topics:
            log_filter["topics"] = topics

        subscribe_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["logs", log_filter]
        }

        self._running = True
        async with websockets.connect(self.uri) as ws:
            self._ws = ws
            await ws.send(json.dumps(subscribe_request))

            response = await ws.recv()
            sub_response = json.loads(response)
            self.subscription_id = sub_response.get("result")
            print(f"Subscribed with ID: {self.subscription_id}")

            try:
                while self._running:
                    message = await ws.recv()
                    event = json.loads(message)

                    if "params" in event and event["params"].get("subscription") == self.subscription_id:
                        log_data = event["params"]["result"]
                        if on_event:
                            on_event(log_data)
                        else:
                            print(f"\n--- New Event ---")
                            print(f"Block: {log_data['blockNumber']}")
                            print(f"Tx Hash: {log_data['transactionHash']}")
                            print(f"Contract: {log_data['address']}")
                            print(f"Topics: {log_data['topics']}")
                            print(f"Data: {log_data['data']}")

            except websockets.exceptions.ConnectionClosed:
                print("Connection closed")
            finally:
                self._ws = None
                self.subscription_id = None

  
    async def _run_subscription(self, subscribe_request, on_event, event_type):
        """Common subscription loop logic."""
        self._running = True
        async with websockets.connect(self.uri) as ws:
            self._ws = ws
            await ws.send(json.dumps(subscribe_request))

            response = await ws.recv()
            sub_response = json.loads(response)
            self.subscription_id = sub_response.get("result")
            print(f"Subscribed with ID: {self.subscription_id}")

            try:
                while self._running:
                    message = await ws.recv()
                    event = json.loads(message)

                    if "params" in event and event["params"].get("subscription") == self.subscription_id:
                        data = event["params"]["result"]
                        if on_event:
                            on_event(data)
                        else:
                            print(f"\n--- New {event_type.title()} ---")
                            print(json.dumps(data, indent=2))

            except websockets.exceptions.ConnectionClosed:
                print("Connection closed")
            finally:
                self._ws = None
                self.subscription_id = None

    async def stop(self):
        """Stop the subscription."""
        self._running = False
        if self._ws:
            await self._ws.close()
