import asyncio
import json
import logging
import sys

# Configure Logging
# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [BRIDGE] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bridge_run.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("OmniLinkBridge")

# Try importing dependencies
try:
    import websockets
    import paho.mqtt.client as mqtt
except ImportError as e:
    logger.critical(f"Missing dependency: {e}")
    logger.critical("Please install required packages: pip install paho-mqtt websockets")
    sys.exit(1)

# Configuration
PONG_RELAY_URI = "ws://localhost:6789/agent"
MQTT_BROKER_HOST = "localhost"
MQTT_BROKER_PORT = 9001
MQTT_TRANSPORT = "websockets"

TOPIC_CONTEXT = "olink/context"
TOPIC_FEEDBACK = "olink/feedback"
TOPIC_COMMANDS = "olink/commands"

# Global State Cache
latest_game_state = None

# --- MQTT Client Setup ---
mqtt_client = mqtt.Client(transport=MQTT_TRANSPORT)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Successfully connected to MQTT Broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        client.subscribe(TOPIC_COMMANDS)
        logger.info(f"Subscribed to {TOPIC_COMMANDS}")
    else:
        logger.error(f"Failed to connect to MQTT, return code {rc}")

def on_publish(client, userdata, mid):
    pass

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode().strip()
        logger.info(f"Received MQTT Command: '{payload_str}'")
        
        # Try parsing as JSON first
        try:
            json_payload = json.loads(payload_str)
            if isinstance(json_payload, dict) and "command" in json_payload:
                payload_str = json_payload["command"]
                logger.info(f"Extracted command from JSON: '{payload_str}'")
        except json.JSONDecodeError:
            pass # Not JSON

        # Handle potential JSON quotes for raw strings
        if payload_str.startswith('"') and payload_str.endswith('"'):
            payload_str = payload_str[1:-1]
            logger.info(f"Stripped quotes: '{payload_str}'")

        # Determine internal JSON to send to Game
        game_payload = None
        
        if payload_str == "reset_score":
            game_payload = {"type": "admin", "command": "reset_score"}
        elif payload_str == "reset_game":
            game_payload = {"type": "admin", "command": "reset_game"}
        elif payload_str == "pause_game":
            game_payload = {"type": "admin", "command": "pause_game"}
        elif payload_str == "resume_game":
            game_payload = {"type": "admin", "command": "resume_game"}
        # Also support direct passthrough if needed, or other commands
        elif payload_str == "pong_move_paddle_up":
             game_payload = {"type": "action", "move": "up"}
        elif payload_str == "pong_move_paddle_down":
             game_payload = {"type": "action", "move": "down"}
        
        if game_payload:
            # We need to send this to the WebSocket. 
            # Since the WS loop is async and this callback is sync, 
            # we'll use the global queue and ensuring it's thread-safe.
            if global_loop and global_loop.is_running():
                asyncio.run_coroutine_threadsafe(command_queue.put(game_payload), global_loop)
            else:
                logger.error("Cannot queue command: global_loop is not running!")
        else:
            logger.debug(f"Ignored unknown command: {payload_str}")
            
    except Exception as e:
        logger.error(f"Error processing MQTT message: {e}")

mqtt_client.on_connect = on_connect
mqtt_client.on_publish = on_publish
mqtt_client.on_message = on_message

# Async Queue for passing commands from MQTT thread to WS loop
command_queue = asyncio.Queue()
global_loop = None

async def start_mqtt():
    logger.info(f"Connecting to MQTT Broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT} ({MQTT_TRANSPORT})...")
    try:
        mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
        mqtt_client.loop_start()  # Run in a background thread
    except Exception as e:
        logger.error(f"MQTT Connection Error: {e}")
        # We don't exit, we might retry or just let the loop fail later, 
        # but for this script, if MQTT fails, we probably want to know.
        raise e

# --- Pong Relay Listener ---
async def pong_relay_listener():
    global latest_game_state
    
    logger.info(f"Attempting connection to Pong Relay at {PONG_RELAY_URI}...")
    while True:
        try:
            async with websockets.connect(PONG_RELAY_URI) as ws:
                logger.info("Connected to Pong Relay! Listening for state and waiting for commands...")
                
                # We need to listen for WS messages AND check the command_queue.
                # Use asyncio.wait for handling both.
                
                # Task A: Receive from WS
                async def receive_ws():
                    async for message in ws:
                        # logger.info(f"DEBUG: Received WS message size: {len(message)}") # Uncomment for verbose debugging
                        try:
                            data = json.loads(message)
                            if data.get("type") == "state":
                                global latest_game_state
                                latest_game_state = data
                                # logger.info("DEBUG: State updated")
                            else:
                                logger.info(f"Received non-state message: {data}")
                        except json.JSONDecodeError:
                            pass
                
                # Task B: Send commands from Queue
                async def send_commands():
                    while True:
                        cmd = await command_queue.get()
                        await ws.send(json.dumps(cmd))
                        logger.info(f"Forwarded command to game: {cmd}")

                # Run both
                done, pending = await asyncio.wait(
                    [asyncio.create_task(receive_ws()), asyncio.create_task(send_commands())],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending:
                    task.cancel()
                    
        except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
            logger.warning(f"Pong Relay disconnected ({e}). Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected Relay Error: {e}")
            await asyncio.sleep(5)

# --- Periodic Publishers ---
async def context_publisher():
    logger.info("Starting Context Publisher (Interval: 10 seconds)...")
    while True:
        await asyncio.sleep(10)
        
        # Publish Game State to Context
        if latest_game_state:
            try:
                payload = json.dumps(latest_game_state)
                info = mqtt_client.publish(TOPIC_CONTEXT, payload)
                # info.wait_for_publish() # Optional: Blocking here might delay the loop slightly
                logger.info(f"Published to '{TOPIC_CONTEXT}': {payload}")
            except Exception as e:
                logger.error(f"Failed to publish context: {e}")
        else:
            logger.warning("Context timer triggered, but no game state received yet.")

async def feedback_publisher():
    logger.info("Starting Feedback Publisher (Interval: 60 seconds)...")
    while True:
        await asyncio.sleep(60)

        # Publish Feedback
        try:
            feedback_data = {"feedback": "check status"}
            feedback_msg = json.dumps(feedback_data)
            info = mqtt_client.publish(TOPIC_FEEDBACK, feedback_msg)
            # info.wait_for_publish()
            logger.info(f"Published to '{TOPIC_FEEDBACK}': {feedback_msg}")
        except Exception as e:
            logger.error(f"Failed to publish feedback: {e}")

async def main():
    global global_loop
    global_loop = asyncio.get_running_loop()
    
    # 1. Start MQTT
    try:
        await start_mqtt()
    except Exception:
        logger.critical("Could not start MQTT. Exiting.")
        return
    
    # Needs loop start context
    # mqtt_client.loop_start() # Already called in start_mqtt
    
    # 2. Run concurrent tasks
    await asyncio.gather(
        pong_relay_listener(),
        context_publisher(),
        feedback_publisher()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bridge stopping by user request...")
        mqtt_client.loop_stop()
        logger.info("Bridge stopped.")
