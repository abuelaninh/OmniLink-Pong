import asyncio, websockets, json
async def test_game():
    async with websockets.connect("ws://localhost:6789/game") as ws:
        print("Connected")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if data.get("type") == "admin":
                print("GOT ADMIN COMMAND:", data["command"])
if __name__ == "__main__":
    asyncio.run(test_game())
