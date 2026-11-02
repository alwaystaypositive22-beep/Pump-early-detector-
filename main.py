




import asyncio
import aiohttp
import websockets
import json
import os
import time
import csv
import html

# GITHUB SECRETS
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
HELIUS_RPC = os.environ["HELIUS_RPC"]

# SETTINGS
PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"

CHECK_DELAY_SEC = 20
MAX_CONCURRENT_CHECKS = 3
MAX_ACTIVE_TASKS = 60

# Minimum değerlendirme puanı
MIN_SCORE = 2

# Her yeni token için bildirim gönderilmez.
# Yalnızca yeterli kontrol sinyali alanlar gönderilir.
LOG_FILE = "token_log.csv"

seen_tokens = set()
active_tasks = set()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)


# -------------------------
# HELIUS RPC WITH RETRIES
# -------------------------

async def rpc(session, method, params):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }

    for attempt in range(5):
        try:
            async with session.post(
                HELIUS_RPC,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:

                if response.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(2 * (attempt + 1))
                    continue

                response.raise_for_status()
                data = await response.json()

                if "error" in data:
                    error = data["error"]
                    message = str(error.get("message", error))

                    temporary_errors = (
                        "overloaded",
                        "could not find account",
                        "not found",
                        "rate limit"
                    )

                    if any(x in message.lower() for x in temporary_errors):
                        await asyncio.sleep(2 * (attempt + 1))
                        continue

                    raise RuntimeError(message)

                return data.get("result")

        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == 4:
                raise

            await asyncio.sleep(2 * (attempt + 1))

    return None


# -------------------------
# TOKEN SUPPLY
# -------------------------

async def get_supply(session, mint):
    for attempt in range(4):
        try:
            result = await rpc(
                session,
                "getTokenSupply",
                [mint]
            )

            if result and result.get("value"):
                value = result["value"]
                amount = value.get("amount")
                decimals = int(value.get("decimals", 0))

                if amount is not None:
                    return int(amount) / (10 ** decimals)

        except Exception as error:
            print("Arz kontrolü:", error)

        await asyncio.sleep(3 * (attempt + 1))

    return None


# -------------------------
# DEV WALLET HISTORY
# -------------------------

async def get_dev_history(session, dev):
    result = await rpc(
        session,
        "getSignaturesForAddress",
        [dev, {"limit": 50}]
    )

    if result is None:
        return None

    return len(result)


# -------------------------
# METADATA
# -------------------------

def normalize_metadata_url(uri):
    if not uri:
        return None

    if uri.startswith("ipfs://"):
        return "https://ipfs.io/ipfs/" + uri[7:]

    if uri.startswith("ar://"):
        return "https://arweave.net/" + uri[5:]

    return uri


async def fetch_metadata(session, uri):
    url = normalize_metadata_url(uri)

    if not url:
        return {}

    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=12)
        ) as response:

            if response.status != 200:
                return {}

            return await response.json(content_type=None)

    except Exception as error:
        print("Metadata hatası:", error)
        return {}


# -------------------------
# TOKEN ANALYSIS
# -------------------------

async def evaluate(session, token):
    mint = token.get("mint")
    dev = token.get("traderPublicKey")

    if not mint or not dev:
        return None

    warnings = []
    missing = []
    score = 0

    # 1. TOKEN SUPPLY
    supply = await get_supply(session, mint)

    if supply is None:
        missing.append("Token arzı henüz alınamadı")
    else:
        score += 1

    # 2. DEV WALLET
    try:
        dev_history = await get_dev_history(session, dev)

        if dev_history is None:
            missing.append("Dev geçmişi alınamadı")

        elif dev_history < 5:
            warnings.append(
                f"Dev cüzdanında son 50 imza içinde yalnızca {dev_history} kayıt var"
            )

        else:
            score += 1

    except Exception as error:
        print("Dev kontrolü:", error)
        dev_history = None
        missing.append("Dev geçmişi kontrol edilemedi")

    # 3. METADATA AND SOCIALS
    metadata = await fetch_metadata(
        session,
        token.get("uri", "")
    )

    socials = {}

    for key in ("twitter", "telegram", "website"):
        value = metadata.get(key)

        if value:
            socials[key] = value

    if socials:
        score += 1
    else:
        warnings.append("Sosyal bağlantı bulunamadı")

    # 4. WAIT FOR EARLY TOKEN DATA
    await asyncio.sleep(CHECK_DELAY_SEC)

    # Missing data is not automatically a scam signal.
    if missing:
        verdict = "VERİ EKSİK"
    elif score >= MIN_SCORE:
        verdict = "İNCELEMEYE DEĞER"
    else:
        verdict = "DÜŞÜK SİNYAL"

    info = {
        "supply": supply,
        "dev_history": dev_history,
        "socials": socials,
        "warnings": warnings,
        "missing": missing,
        "score": score
    }

    return verdict, info


# -------------------------
# TELEGRAM
# -------------------------

async def send_telegram(session, message):
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=20)
    ) as response:

        if response.status != 200:
            error = await response.text()
            raise RuntimeError(error)


def format_message(token, verdict, info):
    mint = token["mint"]

    name = html.escape(
        str(token.get("name") or "Bilinmiyor")
    )

    symbol = html.escape(
        str(token.get("symbol") or "?")
    )

    dev_history = info["dev_history"]

    if dev_history is None:
        dev_text = "Alınamadı"
    else:
        dev_text = str(dev_history)

    supply = info["supply"]

    if supply is None:
        supply_text = "Henüz alınamadı"
    else:
        supply_text = f"{supply:,.0f}"

    lines = [
        "🔎 <b>YENİ PUMP.FUN TOKEN</b>",
        "",
        f"<b>{name} (${symbol})</b>",
        "",
        f"Değerlendirme: {verdict}",
        f"Sinyal puanı: {info['score']}/3",
        f"Token arzı: {supply_text}",
        f"Dev son 50 imza sayısı: {dev_text}",
        f"Market cap: {token.get('marketCapSol', '?')} SOL",
        "",
        "⚠️ Bu sonuç güvenlik garantisi değildir."
    ]

    if info["warnings"]:
        lines += [
            "",
            "<b>Uyarılar:</b>"
        ]

        for warning in info["warnings"]:
            lines.append(html.escape(warning))

    if info["missing"]:
        lines += [
            "",
            "<b>Eksik veriler:</b>"
        ]

        for item in info["missing"]:
            lines.append(html.escape(item))

    if info["socials"]:
        lines += [
            "",
            "<b>Sosyal bağlantılar:</b>"
        ]

        for key, value in info["socials"].items():
            lines.append(
                f"{html.escape(key)}: {html.escape(str(value))}"
            )

    lines += [
        "",
        f"<code>{html.escape(mint)}</code>",
        "",
        f"https://pump.fun/coin/{mint}",
        f"https://dexscreener.com/solana/{mint}",
        f"https://rugcheck.xyz/tokens/{mint}"
    ]

    return "\n".join(lines)


# -------------------------
# CSV LOGGING
# -------------------------

def log_token(token, verdict, info):
    new_file = not os.path.exists(LOG_FILE)

    try:
        with open(
            LOG_FILE,
            "a",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.writer(file)

            if new_file:
                writer.writerow([
                    "timestamp",
                    "mint",
                    "symbol",
                    "market_cap_sol",
                    "supply",
                    "dev_history",
                    "score",
                    "verdict"
                ])

            writer.writerow([
                int(time.time()),
                token.get("mint"),
                token.get("symbol"),
                token.get("marketCapSol"),
                info.get("supply"),
                info.get("dev_history"),
                info.get("score"),
                verdict
            ])

    except Exception as error:
        print("CSV hatası:", error)


# -------------------------
# PROCESS TOKEN
# -------------------------

async def handle_token(session, token):
    mint = token.get("mint")

    if not mint:
        return

    async with semaphore:
        try:
            result = await evaluate(session, token)

            if result is None:
                return

            verdict, info = result

            log_token(token, verdict, info)

            print(
                f"{token.get('symbol', '?')}: "
                f"{verdict} | Puan: {info['score']}/3"
            )

            # Notify only when enough signals exist.
            if verdict == "İNCELEMEYE DEĞER":
                message = format_message(
                    token,
                    verdict,
                    info
                )

                await send_telegram(session, message)

                print(
                    f"{token.get('symbol', '?')}: "
                    "Telegram bildirimi gönderildi."
                )

        except Exception as error:
            print(
                f"Token işleme hatası ({mint}): {error}"
            )


# -------------------------
# MAIN WEBSOCKET LOOP
# -------------------------

async def main():
    timeout = aiohttp.ClientTimeout(total=25)

    connector = aiohttp.TCPConnector(
        limit=15,
        limit_per_host=5
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        while True:
            try:
                async with websockets.connect(
                    PUMPPORTAL_WS,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10
                ) as ws:

                    await ws.send(
                        json.dumps({
                            "method": "subscribeNewToken"
                        })
                    )

                    print(
                        "Pump.fun yeni token takibi aktif."
                    )

                    async for raw in ws:
                        try:
                            token = json.loads(raw)

                            if token.get("txType") != "create":
                                continue

                            mint = token.get("mint")

                            if not mint or mint in seen_tokens:
                                continue

                            if len(active_tasks) >= MAX_ACTIVE_TASKS:
                                print(
                                    "Analiz kuyruğu dolu, "
                                    "token atlandı."
                                )
                                continue

                            seen_tokens.add(mint)

                            task = asyncio.create_task(
                                handle_token(session, token)
                            )

                            active_tasks.add(task)
                            task.add_done_callback(
                                active_tasks.discard
                            )

                        except json.JSONDecodeError:
                            print("Geçersiz WebSocket mesajı.")

            except Exception as error:
                print(
                    f"WebSocket bağlantı hatası: {error}"
                )

                await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
