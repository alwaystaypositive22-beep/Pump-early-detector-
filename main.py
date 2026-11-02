
import asyncio
import json
import os
import urllib.request
import websockets

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

WS_URL = "wss://pumpportal.fun/api/data"

def send_telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = json.dumps({
        "chat_id": CHAT_ID,
        "text": message,
        "disable_web_page_preview": True
    }).encode()

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"}
    )

    with urllib.request.urlopen(req, timeout=15) as response:
        return response.read()

async def listen():
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                await ws.send(json.dumps({
                    "method": "subscribeNewToken"
                }))

                print("Pump.fun token takibi aktif.")

                while True:
                    raw = await ws.recv()
                    token = json.loads(raw)

                    mint = token.get("mint")
                    name = token.get("name", "Bilinmiyor")
                    symbol = token.get("symbol", "Bilinmiyor")
                    creator = token.get("traderPublicKey", "Bilinmiyor")

                    if not mint:
                        continue

                    message = (
                        "🚨 YENİ PUMP.FUN TOKENI\n\n"
                        f"İsim: {name}\n"
                        f"Ticker: ${symbol}\n"
                        f"Contract:\n{mint}\n\n"
                        f"Creator: {creator}\n\n"
                        f"Pump.fun: https://pump.fun/coin/{mint}"
                    )

                    await asyncio.to_thread(
                        send_telegram, message
                    )

        except Exception as error:
            print(f"Bağlantı hatası: {error}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(listen())
