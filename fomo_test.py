

import os
import asyncio
import aiohttp
import json

API_KEY = os.environ["FOMO_API_KEY"]
URL = "https://api.fomoapi.io/v2/leaderboard/30d?limit=20"


async def main():
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(URL, headers=headers) as response:
            print("HTTP DURUM:", response.status)

            data = await response.json(content_type=None)

            if response.status != 200:
                print(json.dumps(data, indent=2, ensure_ascii=False))
                return

            traders = data.get("traders", [])

            print("VERİ KAYNAĞI:", data.get("source"))
            print("VERİ ZAMANI:", data.get("capturedAt"))
            print("BULUNAN TRADER:", len(traders))

            for trader in traders:
                wallets = trader.get("wallets") or {}

                print("\n-------------------------")
                print("SIRA:", trader.get("rank"))
                print("TRADER:", trader.get("handle"))
                print("PNL ($):", trader.get("pnlUsd"))
                print("HACİM ($):", trader.get("volumeUsd"))
                print("İŞLEM SAYISI:", trader.get("trades"))
                print("SOLANA CÜZDANI:", wallets.get("solana"))


if __name__ == "__main__":
    asyncio.run(main())
