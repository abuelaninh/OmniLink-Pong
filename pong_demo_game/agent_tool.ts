/**
 * Agent Tool for Pong Demo
 * 
 * Target: Browser (EcmaScript Module / OmniLink UI)
 * 
 * Responsibilities:
 * 1. Establish direct WebSocket API to ws://localhost:6789/agent.
 * 2. Parse out fast-refreshing game state (ball, paddles).
 * 3. Decide optimal vertical movement (up/down/stop).
 * 4. Dispatch `action` payload stream over the WebSocket.
 */

interface GameState {
    type: "state";
    ball: { x: number, y: number, vx: number, vy: number };
    leftPaddleY: number;
    rightPaddleY: number;
    score: { left: number, right: number };
}

interface AgentAction {
    type: "action";
    move: "up" | "down" | "stop";
}

const WS_URL = "ws://localhost:6789/agent";

function startAgentLoop() {
    console.log(`🚀 Pong Browser Agent Started. Attempting connection to ${WS_URL}...`);

    const ws = new WebSocket(WS_URL);

    ws.onopen = () => {
        console.log("✅ [AGENT ONLINE]: Successfully linked to the Pong Game Server!");
    };

    ws.onmessage = (event: MessageEvent) => {
        try {
            const gameState: GameState = JSON.parse(event.data);

            if (gameState.type !== "state") {
                return; // Ignore other broadcast types
            }

            const paddleHeight = 80;
            const paddleCenter = gameState.leftPaddleY + (paddleHeight / 2);
            const ballY = gameState.ball.y;
            const tolerance = 10; // Pixels

            let moveCmd: "up" | "down" | "stop" = "stop";

            if (paddleCenter < ballY - tolerance) {
                moveCmd = "down";
            } else if (paddleCenter > ballY + tolerance) {
                moveCmd = "up";
            }

            // Only log and send if we are actively moving to reduce spam
            if (moveCmd !== "stop") {
                // console.log(`[AGENT] Ball @ ${Math.round(ballY)} | Paddle @ ${Math.round(paddleCenter)} -> Action: ${moveCmd.toUpperCase()}`);

                const actionPayload: AgentAction = {
                    type: "action",
                    move: moveCmd
                };
                ws.send(JSON.stringify(actionPayload));
            }
        } catch (error) {
            console.error("❌ Link parse error or pipeline fail:", error);
        }
    };

    ws.onclose = () => {
        console.log("🔌 [DISCONNECTED]: Dropped socket back to host. Retrying loop in 2s...");
        setTimeout(startAgentLoop, 2000);
    };

    ws.onerror = (error: Event) => {
        console.error("⚠️ WebSocket Connectivity Event Warning Detected:");
    };
}

// Bootstrap loop trigger
startAgentLoop();
