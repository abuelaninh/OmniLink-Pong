"""
Pong agent WebSocket client

This script connects to the Pong WebSocket relay server at ws://localhost:6789/agent.
It listens for state messages from the game and responds with action messages
to move the left paddle in an attempt to intercept the ball.  The agent
computes a simple heuristic: if the centre of its paddle is above the ball,
it moves the paddle down; if below, it moves up.  A tolerance zone prevents
over–steering.  This yields near–optimal play without relying on the built–in
AI.

Usage:

    python agent_client.py

Make sure the WebSocket relay (`ws_server.py`) is running and that the
game is connected to the `/game` endpoint.
"""

import asyncio
import json
import websockets


async def run_agent(host: str = "localhost", port: int = 6789) -> None:
    uri = f"ws://{host}:{port}/agent"
    async with websockets.connect(uri) as ws:
        print(f"Connected to Pong server at {uri}")
        # Paddle height is fixed at 80px in the game; adjust if changed
        paddle_height = 80
        tolerance = 4  # pixel tolerance before moving
        # Payload definitions to match OmniLink Spec
        PATTERNS = {
            "pong_move_paddle_up": {"type": "action", "move": "up"},
            "pong_move_paddle_down": {"type": "action", "move": "down"}
        }

        try:
            async for message in ws:
                data = json.loads(message)
                if data.get("type") != "state":
                    continue
                ball_y = data["ball"]["y"]
                paddle_y = data["leftPaddleY"]
                paddle_centre = paddle_y + paddle_height / 2
                
                command_key = None
                if paddle_centre < ball_y - tolerance:
                    command_key = "pong_move_paddle_down"
                elif paddle_centre > ball_y + tolerance:
                    command_key = "pong_move_paddle_up"
                
                # Send movement command if needed
                if command_key:
                    action = PATTERNS[command_key]
                    await ws.send(json.dumps(action))
        except websockets.ConnectionClosed:
            print("Disconnected from server")


if __name__ == "__main__":
    asyncio.run(run_agent())