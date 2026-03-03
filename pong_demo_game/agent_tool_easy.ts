/**
 * Agent Tool for Pong Demo (EASY MODE)
 * 
 * Target: Browser (EcmaScript Module / OmniLink UI)
 * 
 * Responsibilities:
 * 1. Establish direct WebSocket API to ws://localhost:6789/agent.
 * 2. Parse out fast-refreshing game state (ball, paddles).
 * 3. Decide optimal vertical movement with deliberate mistakes.
 * 4. Dispatch `action` payload stream over the WebSocket.
 */

interface GameStateEasy {
    type: "state";
    ball: { x: number, y: number, vx: number, vy: number };
    leftPaddleY: number;
    rightPaddleY: number;
    score: { left: number, right: number };
}

interface AgentActionEasy {
    type: "action";
    move: "up" | "down" | "stop";
}

const WS_URL_EASY = "ws://localhost:6789/agent";

function startAgentLoopEasy() {
    console.log(`🚀 Pong Browser Agent Started (EASY MODE). Attempting connection to ${WS_URL_EASY}...`);

    const ws = new WebSocket(WS_URL_EASY);

    ws.onopen = () => {
        console.log("✅ [AGENT-EASY ONLINE]: Successfully linked to the Pong Game Server!");
    };

    ws.onmessage = (event: MessageEvent) => {
        try {
            const gameState: GameStateEasy = JSON.parse(event.data);

            if (gameState.type !== "state") {
                return;
            }

            const paddleHeight = 80;
            const paddleCenter = gameState.leftPaddleY + (paddleHeight / 2);
            const ballY = gameState.ball.y;
            const tolerance = 35; // Pixels (Higher for easy mode)

            let moveCmd: "up" | "down" | "stop" = "stop";

            if (paddleCenter < ballY - tolerance) {
                moveCmd = "down";
            } else if (paddleCenter > ballY + tolerance) {
                moveCmd = "up";
            }

            // 15% chance to just do nothing or do the opposite (Dumb Mistake)
            if (Math.random() < 0.15) {
                const randomChoice = Math.random();
                if (randomChoice < 0.5) {
                    moveCmd = "stop"; // Hesitate
                } else if (moveCmd === "up") {
                    moveCmd = "down"; // Move wrong way
                } else if (moveCmd === "down") {
                    moveCmd = "up"; // Move wrong way
                }
            }

            if (moveCmd !== "stop") {
                // console.log(`[AGENT-EASY] Ball @ ${Math.round(ballY)} | Paddle @ ${Math.round(paddleCenter)} -> Action: ${moveCmd.toUpperCase()}`);

                const actionPayload: AgentActionEasy = {
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
        setTimeout(startAgentLoopEasy, 2000);
    };

    ws.onerror = (error: Event) => {
        console.error("⚠️ WebSocket Connectivity Event Warning Detected:");
    };
}

// Bootstrap loop trigger
startAgentLoopEasy();
