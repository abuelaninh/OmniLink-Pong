# Codebase Guide

This document explains every file in the Pong Demo project to help developers easily understand what each file does, how it connects to the system, and what its specific responsibilities are.

## Directory Structure Overview

Below is an outline of the key files in this project:

```text
pong_demo_game/
├── README.md               # High-level overview, architecture, and run instructions
├── CODE_EXPLANATION.md     # This file; detailed file-by-file explanations
├── pong.html                 # The game client and environment (frontend)
├── ws_server.py              # The WebSocket Relay Server (backend nervous system)
├── omnilink_bridge.py        # The MQTT to WebSocket Bridge (middleware)
├── http_proxy.py             # HTTP proxy wrapper around the WS stream
├── agent_client.py           # Example standalone Python AI Agent
├── agent_tool.ts             # TypeScript browser-based AI Agent
├── agent_tool_easy.ts        # Variation of the TypeScript Agent
├── agent_runner.html         # Test runner for Browser/TS Agents
├── mosquitto.conf            # Configuration for the MQTT broker
└── tsconfig.json             # TypeScript compiler rules
```

---

## Core Infrastructure Files

### `ws_server.py`
**The Core Relay Router**
This is the heart of the project. It handles all fast, real-time message switching using `websockets` and `asyncio`.
- **Role**: Maintains persistent connections with the Game (`/game`) and any AI Agents (`/agent`).
- **What it does**: 
  1. Accepts JSON `state` broadcasts from the game and blasts them to every connected agent.
  2. Accepts JSON `action` broadcasts from agents and pushes them to the game.
  3. Acts as an MQTT client to publish the score difference to `olink/context` every 20 seconds.
  4. Listens for admin override commands via MQTT (`olink/commands`) to reset or pause the game.

### `pong.html`
**The Environment (Frontend)**
This is a standard browser-based HTML5 Canvas game with no backend logic built-in.
- **Role**: Computes physics, renders the ball/paddles, and acts as the "source of truth" for the game state.
- **What it does**:
  1. Opens a WebSocket to `ws://localhost:6789/game`.
  2. Computes collision logic at 60 Frames Per Second (FPS).
  3. Controls the right paddle with built-in heuristic AI (with intentional "mistake" timers to make it beatable).
  4. Yields control of the left paddle entirely to WebSocket commands.
  5. Serializes every frame into a JSON payload (`{"type": "state", ...}`) and sends it to the relay.

---

## Middleware & Bridges

### `omnilink_bridge.py`
**The Protocol Converter**
This file bridges the fast, ephemeral WebSocket stream with the publish/subscribe messaging system (MQTT).
- **Role**: Syncs game data to the wider OmniLink architecture network.
- **What it does**:
  1. Connects to `ws://localhost:6789/agent` acting as an invisible "eavesdropping agent".
  2. Caches the latest game state.
  3. Publishes the full raw game state JSON to `olink/context` on MQTT every 10 seconds.
  4. Subscribes to `olink/commands`. If a command like "pause_game" arrives, the bridge catches it in a synchronous MQTT thread, safely ports it to an `asyncio.Queue` using `run_coroutine_threadsafe`, and forwards an `admin` payload down the WebSocket to the game.

### `http_proxy.py`
**The RESTful Wrapper**
Sometimes AI agents can't keep a persistent WebSocket open or prefer standard HTTP `GET` / `POST` calls. This file enables that.
- **Role**: Converts the WebSocket event stream into standard REST APIs.
- **What it does**:
  1. Keeps a WebSocket open to intercept game state frames.
  2. Hosts a threaded HTTP server on port `5000`.
  3. Re-exposes the cached state on `GET /data`.
  4. Accepts commands via `POST /callback` and forwards those directions into the WebSocket logic loop.

---

## The Agents (The Players)

### `agent_client.py`
**Python Heuristic Agent**
A script representing an independent AI player connecting externally.
- **Role**: Plays the game.
- **What it does**:
  1. Connects to `ws://localhost:6789/agent`.
  2. Parses the `leftPaddleY` and `ball_y` from the incoming frames.
  3. Uses a simple algorithm: If the ball is above the paddle center, output `pong_move_paddle_up`; if below, output `pong_move_paddle_down`.

### `agent_tool.ts` & `agent_tool_easy.ts`
**Browser TypeScript Agents**
These are specialized versions of the agent designed to run directly inside a browser context, primarily so the OmniLink UI tool panels can hijack control without needing a Python backend script running.
- **Role**: Play the game, but doing so using the browser's native `WebSocket` API.
- **Difference**: `_easy.ts` might feature varied tolerance values or intentional delays to simulate easier difficulties while testing agent intelligence.

### `agent_runner.html`
**Agent Sandbox Environment**
A simple HTML wrapper. If you want to use the `.ts` agents, you need a browser runtime. This file acts as the host tab so you don't have to embed the TS code directly into the main `pong.html` file (keeping the game totally decoupled from the AI).

---

## Configuration Files

### `mosquitto.conf`
**MQTT configuration**
- **Role**: Tells the local Eclipse Mosquitto broker to use WebSockets (`protocol websockets`) on port `9001` rather than raw TCP sockets. This is mandatory since both Python and standard browser apps are talking to the MQTT broker using WS transport layers.

### `tsconfig.json`
**TypeScript Compiler Rules**
- **Role**: Instructions for how `tsc` converts the `agent_tool.ts` files into flat Javascript so browsers can execute them.
