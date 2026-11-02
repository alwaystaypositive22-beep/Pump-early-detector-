

import asyncio
import json
import os
import time
import urllib.request
import urllib.error
import websockets

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

WS_URL = "wss://pumpportal.fun/api/data"
DEX_URL = "https://api.dexscreener.com/token-pairs/v1/solana/"

sent_tokens = set()
active_tasks = set()
semaphore = asyncio.Semaphore(4)


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

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        print("Telegram hatası:", error.read().decode())
        raise


def get_pairs(mint):
    req = urllib.request.Request(
        DEX_URL + mint,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode())
    except Exception as error:
        print("DEX verisi alınamadı:", error)
        return []


def analyze_pair(pair):
    liquidity = (pair.get("liquidity") or {}).get("usd") or 0

    volume = pair.get("volume") or {}
    volume_5m = volume.get("m5") or 0

    txns = pair.get("txns") or {}
    txns_5m = txns.get("m5") or {}

    buys = txns_5m.get("buys") or 0
    sells = txns_5m.get("sells") or 0
    total = buys + sells

    score = 0

    if liquidity >= 20000:
        score += 30
    elif liquidity >= 10000:
        score += 24
    elif liquidity >= 5000:
        score += 16
    elif liquidity >= 2000:
        score += 8

    if volume_5m >= 5000:
        score += 20
    elif volume_5m >= 1000:
        score += 15
    elif volume_5m >= 250:
        score += 8

    if total >= 50:
        score += 20
    elif total >= 20:
        score += 15
    elif total >= 5:
        score += 8

    if total > 0:
        buy_ratio = buys / total

        if buy_ratio >= 0.65:
            score += 20
        elif buy_ratio >= 0.55:
            score += 15
        elif buy_ratio > 0.50:
            score += 10

    return score, liquidity, volume_5m, buys, sells


async def check_token(token):
    mint = token.get("mint")
    name = token.get("name", "Bilinmiyor")
    symbol = token.get("symbol", "Bilinmiyor")
    creator = token.get("traderPublicKey", "Bilinmiyor")

    if not mint:
        return

    delays = [30, 60, 120, 300, 600, 900]
    start = time.monotonic()

    async with semaphore:
        for delay in delays:
            wait = delay - (time.monotonic() - start)

            if wait > 0:
                await asyncio.sleep(wait)

            try:
                pairs = await asyncio.to_thread(get_pairs, mint)

                if not pairs:
                    print(f"{symbol}: Henüz DEX verisi yok.")
                    continue

                # En yüksek likiditeli havuzu seç
                pair = max(
                    pairs,
                    key=lambda p: (p.get("liquidity") or {}).get("usd") or 0
                )

                score, liquidity, volume, buys, sells = analyze_pair(pair)

                print(f"{symbol}: Momentum puanı {score}/100")

                if score >= 65 and mint not in sent_tokens:
                    sent_tokens.add(mint)

                    message = (
                        "🚨 ERKEN TOKEN ADAYI\n\n"
                        f"İsim: {name}\n"
                        f"Ticker: ${symbol}\n"
                        f"Momentum: {score}/100\n\n"
                        f"Likidite: ${liquidity:,.0f}\n"
                        f"5 dk hacim: ${volume:,.0f}\n"
                        f"5 dk alım: {buys}\n"
                        f"5 dk satım: {sells}\n\n"
                        f"Creator: {creator}\n"
                        f"Contract:\n{mint}\n\n"
                        f"DEX Screener: {pair.get('url', 'Veri yok')}\n"
                        f"Pump.fun: https://pump.fun/coin/{mint}\n\n"
                        "Not: Bu puan momentum göstergesidir, "
                        "rug güvenliği garantisi değildir."
                    )

                    await asyncio.to_thread(send_telegram, message)
                    print(f"{symbol}: Telegram bildirimi gönderildi.")
                    return

            except Exception as error:
                print(f"{symbol} analiz hatası: {error}")


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

                    if not mint or mint in sent_tokens:
                        continue

                    if len(active_tasks) >= 100:
                        print("Analiz kuyruğu dolu, token atlandı.")
                        continue

                    task = asyncio.create_task(check_token(token))
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)

        except Exception as error:
            print(f"Bağlantı hatası: {error}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(listen())

