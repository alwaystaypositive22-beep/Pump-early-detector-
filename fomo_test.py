
import os
import asyncio
import aiohttp
import json

API_KEY = os.environ["FOMO_API_KEY"]

URL = "https://api.fomoapi.io/v2/leaderboard/30d?limit=5"


async def main():
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(
            URL,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:

            print("HTTP durum kodu:", response.status)

            data = await response.json()

            if response.status != 200:
                print("API hatası:", json.dumps(data, indent=2))
                return

            traders = data.get("traders", [])

            print("\n30 GÜNLÜK TRADER LEADERBOARD")
            print("Bulunan trader:", len(traders))

            for trader in traders:
                print("\n--------------------")
                print("Sıra:", trader.get("rank"))
                print("Handle:", trader.get("handle"))
                print("User ID:", trader.get("userId"))
                print("Veri:", json.dumps(trader, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
