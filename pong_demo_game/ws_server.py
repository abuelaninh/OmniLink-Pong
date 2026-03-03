"""
WebSocket relay server for the Pong demo (with MQTT Context Publishing & Command Subscription).

This server listens on localhost:6789 and allows two types of clients to connect:

* The **game** client connects at path `/game`.
* The **agent** client connects at path `/agent`.

Features:
- Relays messages between Game and Agent.
- Parses Game State to track scores.
- Publishes Score Difference (Agent - AI) to MQTT topic 'olink/context' every 20s.
- Subscribes to MQTT topic 'olink/commands' to handle 'reset_game' and 'reset_score'.
"""

import asyncio
import json
import logging
from typing import Dict, Optional
import sys

import websockets

# --- MQTT Setup ---
try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("CRITICAL: paho-mqtt not installed. Please install it: pip install paho-mqtt")
    sys.exit(1)

MQTT_BROKER_HOST = "localhost"
MQTT_BROKER_PORT = 9001
MQTT_TOPIC_CONTEXT = "olink/context"
MQTT_TOPIC_COMMANDS = "olink/commands"
MQTT_TRANSPORT = "websockets"

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [WS-SERVER] - %(message)s')
logger = logging.getLogger("PongServer")

class PongRelayServer:
    """Relay between game and agent + MQTT Context Publisher & Command Subscriber."""

    def __init__(self, host: str = "localhost", port: int = 6789) -> None:
        self.host = host
        self.port = port
        self.clients: Dict[str, websockets.WebSocketServerProtocol] = {}
        
        # State Tracking
        self.left_score = 0
        self.right_score = 0
        
        # Runtime Loop Reference (captured in run())
        self.loop = None
        
        # MQTT Client
        self.mqtt_client = mqtt.Client(transport=MQTT_TRANSPORT)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT Broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
            client.subscribe(MQTT_TOPIC_COMMANDS)
            logger.info(f"Subscribed to {MQTT_TOPIC_COMMANDS}")
        else:
            logger.error(f"Failed to connect to MQTT, return code {rc}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        logger.warning(f"Disconnected from MQTT (rc={rc})")

    def on_mqtt_message(self, client, userdata, msg):
        """Handle incoming MQTT commands."""
        try:
            raw_payload = msg.payload.decode()
            cleaned_payload = raw_payload.strip()
            logger.info(f"DEBUG: [MQTT] Raw received: '{raw_payload}', Cleaned: '{cleaned_payload}'")
            
            # Try parsing as JSON first (User reported JSON payloads)
            try:
                json_payload = json.loads(cleaned_payload)
                if isinstance(json_payload, dict) and "command" in json_payload:
                    cleaned_payload = json_payload["command"]
                    logger.info(f"DEBUG: [MQTT] Extracted command from JSON: '{cleaned_payload}'")
            except json.JSONDecodeError:
                pass # Not JSON, treat as raw string

            # Check for potential extra quotes if it wasn't valid JSON
            if cleaned_payload.startswith('"') and cleaned_payload.endswith('"'):
                cleaned_payload = cleaned_payload[1:-1]
                logger.info(f"DEBUG: [MQTT] Removed quotes, new payload: '{cleaned_payload}'")

            if cleaned_payload in ["reset_game", "reset_score", "pause_game", "resume_game"]:
                logger.info(f"DEBUG: [MQTT] Recognized command '{cleaned_payload}'. Converting to async task...")
                
                # Check Loop State
                if self.loop:
                    if self.loop.is_running():
                        logger.info("DEBUG: [Async] Loop is running. Scheduling 'send_admin_command'...")
                        future = asyncio.run_coroutine_threadsafe(self.send_admin_command(cleaned_payload), self.loop)
                        
                        # verify future completion (optional, adds complex non-blocking checks, skipping for now)
                    else:
                        logger.error("DEBUG: [Async] self.loop is NOT running!")
                else:
                    logger.error("DEBUG: [Async] self.loop is None!")
            else:
                logger.debug(f"DEBUG: [MQTT] Ignored unknown command: {cleaned_payload}")
                
        except Exception as e:
            logger.error(f"DEBUG: [MQTT] Error processing message: {e}")

    async def send_admin_command(self, command_str: str):
        """Sends the admin command to the connected game client."""
        logger.info(f"DEBUG: [Async] send_admin_command('{command_str}') called.")
        
        if "game" in self.clients:
            try:
                # Map payload to Game Protocol
                msg = {"type": "admin", "command": command_str}
                logger.info(f"DEBUG: [Async] Sending to Game Client: {msg}")
                await self.clients["game"].send(json.dumps(msg))
                logger.info(f"DEBUG: [Async] Forwarded '{command_str}' to Game Client successfully.")
                
                # Reset local score tracking if needed
                if command_str == "reset_game" or command_str == "reset_score":
                    self.left_score = 0
                    self.right_score = 0
                    logger.info("DEBUG: [Async] Local scores reset.")
                    
            except websockets.ConnectionClosed:
                logger.warning("DEBUG: Game client disconnected during command send.")
            except Exception as e:
                 logger.error(f"DEBUG: [Async] Error sending to game: {e}")
        else:
            logger.warning(f"DEBUG: Received '{command_str}' but NO Game Client is connected. Current clients: {list(self.clients.keys())}")

    async def start_mqtt(self):
        try:
            self.mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
            self.mqtt_client.loop_start() # Run in background thread
        except Exception as e:
            logger.error(f"Could not connect to MQTT Broker: {e}")

    async def mqtt_publisher_loop(self):
        """Publishes context every 20 seconds."""
        logger.info(f"Starting MQTT Publisher (Topic: {MQTT_TOPIC_CONTEXT}, Interval: 20s)...")
        while True:
            await asyncio.sleep(20)
            try:
                # Calculate Score Difference (Agent - AI)
                score_diff = self.left_score - self.right_score
                
                self.mqtt_client.publish(MQTT_TOPIC_CONTEXT, str(score_diff))
                logger.info(f"Published Context: {score_diff}")
                
            except Exception as e:
                logger.error(f"MQTT Publish Error: {e}")

    async def handler(self, websocket: websockets.WebSocketServerProtocol) -> None:
        """Handle incoming WebSocket connections."""
        path = websocket.request.path
        role = None
        
        if path == "/game":
            role = "game"
        elif path == "/agent":
            role = "agent"
        else:
            await websocket.close(code=4000, reason="Unknown role")
            return

        # Register
        if role == "game":
            self.clients["game"] = websocket
        elif role == "agent":
            if "agents" not in self.clients:
                self.clients["agents"] = set()
            self.clients["agents"].add(websocket)
        
        logger.info(f"Client connected: {role}")

        try:
            async for message in websocket:
                # 1. State Parsing (Snoop on the messages)
                if role == "game":
                    try:
                        data = json.loads(message)
                        if data.get("type") == "state":
                            # Extract scores
                            scores = data.get("score", {})
                            self.left_score = scores.get("left", 0)
                            self.right_score = scores.get("right", 0)
                    except Exception:
                        pass # Don't break relay if parse fails

                # 2. Relay Logic
                if role == "game" and "agents" in self.clients:
                    for agent_ws in list(self.clients["agents"]):
                        try:
                            await agent_ws.send(message)
                        except websockets.ConnectionClosed:
                            pass
                elif role == "agent" and "game" in self.clients:
                    await self.clients["game"].send(message)
                    
        except websockets.ConnectionClosed:
            pass
        finally:
            if role == "game":
                if self.clients.get("game") is websocket:
                    del self.clients["game"]
            elif role == "agent" and "agents" in self.clients:
                self.clients["agents"].discard(websocket)
            logger.info(f"Client disconnected: {role}")

    async def run(self) -> None:
        # Capture current loop for threadsafe calls
        self.loop = asyncio.get_running_loop()
        
        # Start MQTT
        await self.start_mqtt()
        
        # Start Server & Publisher
        await asyncio.gather(
            self.mqtt_publisher_loop(),
            websockets.serve(self.handler, self.host, self.port)
        )

if __name__ == "__main__":
    server = PongRelayServer()
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logger.info("Server stopped.")