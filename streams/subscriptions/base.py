"""
    This is the base class for creating a subscription via eth_subscribe on available rpc nodes that allow websockets
    Right now it only has subscribe_to_logs but you can subscribe to other things too like pending transactions
"""
import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)


class EthSubscription:

    # Reconnection settings
    MAX_RETRIES = 10
    BASE_DELAY = 1  # seconds
    MAX_DELAY = 60  # seconds

    def __init__(self, uri, api_key=None, auto_reconnect=True):
        self.uri = uri
        self.api_key = api_key
        self.subscription_id = None
        self._ws = None
        self._running = False
        self.auto_reconnect = auto_reconnect
        self._retry_count = 0

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

        await self._run_with_reconnect(subscribe_request, on_event, "log")

    def _get_backoff_delay(self) -> float:
        """Calculate exponential backoff delay with jitter."""
        delay = min(self.BASE_DELAY * (2 ** self._retry_count), self.MAX_DELAY)
        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * (hash(str(self._retry_count)) % 100) / 100 - 1)
        return delay + jitter

    async def _run_with_reconnect(self, subscribe_request, on_event, event_type):
        """Run subscription with automatic reconnection on failure."""
        self._running = True
        self._retry_count = 0

        while self._running:
            try:
                await self._run_subscription(subscribe_request, on_event, event_type)
                # If we get here normally (stopped), don't reconnect
                if not self._running:
                    break
            except Exception as e:
                if not self._running or not self.auto_reconnect:
                    break

                self._retry_count += 1
                if self._retry_count > self.MAX_RETRIES:
                    logger.error(f"Max retries ({self.MAX_RETRIES}) exceeded, giving up")
                    break

                delay = self._get_backoff_delay()
                logger.warning(f"Connection lost, reconnecting in {delay:.1f}s (attempt {self._retry_count}/{self.MAX_RETRIES})")
                await asyncio.sleep(delay)

        self._running = False

    async def _run_subscription(self, subscribe_request, on_event, event_type):
        """Common subscription loop logic."""
        async with websockets.connect(self.uri) as ws:
            self._ws = ws
            await ws.send(json.dumps(subscribe_request))

            response = await ws.recv()
            sub_response = json.loads(response)

            if "error" in sub_response:
                error = sub_response["error"]
                logger.error(f"Subscription error: {error.get('message', error)}")
                raise Exception(f"Subscription failed: {error}")

            self.subscription_id = sub_response.get("result")
            logger.info(f"Subscribed with ID: {self.subscription_id}")
            self._retry_count = 0  # Reset on successful connection

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

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed: code={e.code}, reason={e.reason or 'none'}")
                raise  # Re-raise to trigger reconnection
            finally:
                self._ws = None
                self.subscription_id = None

    async def stop(self):
        """Stop the subscription and prevent reconnection."""
        logger.info("Stopping subscription...")
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass  # Already closed
