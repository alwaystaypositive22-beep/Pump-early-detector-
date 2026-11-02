



import asyncio
import aiohttp
import websockets
import json
import os
import time
import csv
import html

# --------------------------------------------------
# AYARLAR
# --------------------------------------------------

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
HELIUS_RPC = os.environ["HELIUS_RPC"]

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"

CHECK_DELAY_SEC = 25
MAX_DEV_BUY_PCT = 10.0
MAX_TOP10_ACCOUNTS_PCT = 35.0
MIN_DEV_WALLET_TX = 5

REQUIRE_SOCIALS = False
SEND_SUSPICIOUS = False

MAX_CONCURRENT_CHECKS = 5
MAX_ACTIVE_TASKS = 100
LOG_FILE = "token_log.csv"

# --------------------------------------------------
# DURUM
# --------------------------------------------------

seen_tokens = set()
active_tasks = set()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)


# --------------------------------------------------
# HELIUS RPC
# --------------------------------------------------

async def rpc(session, method, params):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }

    async with session.post(
        HELIUS_RPC,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=15)
    ) as response:

        response.raise_for_status()
        data = await response.json()

        if "error" in data:
            raise RuntimeError(str(data["error"]))

        return data.get("result")


async def dev_wallet_tx_count(session, dev):
    result = await rpc(
        session,
        "getSignaturesForAddress",
        [dev, {"limit": 50}]
    )

    if result is None:
        return None

    # Bu sayı toplam geçmiş değil,
    # son 50 imza içindeki işlem sayısıdır.
    return len(result)


async def fetch_metadata(session, uri):
    if not uri:
        return {}

    try:
        async with session.get(
            uri,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as response:

            if response.status != 200:
                return {}

            return await response.json(content_type=None)

    except Exception as error:
        print("Metadata hatası:", error)
        return {}


async def get_supply(session, mint):
    result = await rpc(
        session,
        "getTokenSupply",
        [mint]
    )

    if not result or not result.get("value"):
        return None

    value = result["value"]

    ui_amount = value.get("uiAmount")

    if ui_amount is not None:
        return float(ui_amount)

    amount = value.get("amount")
    decimals = int(value.get("decimals", 0))

    if amount is None:
        return None

    return int(amount) / (10 ** decimals)


async def top10_account_percent(
    session,
    mint,
    bonding_curve
):
    largest, curve_accounts, supply = await asyncio.gather(
        rpc(
            session,
            "getTokenLargestAccounts",
            [mint]
        ),
        rpc(
            session,
            "getTokenAccountsByOwner",
            [
                bonding_curve,
                {"mint": mint},
                {"encoding": "jsonParsed"}
            ]
        ),
        get_supply(session, mint)
    )

    if not largest or not curve_accounts or not supply:
        return None

    curve_addresses = {
        account["pubkey"]
        for account in curve_accounts.get("value", [])
    }

    accounts = largest.get("value", [])

    # Bonding curve hesaplarını hariç tut.
    holders = [
        account
        for account in accounts
        if account["address"] not in curve_addresses
    ]

    top10_amount = sum(
        float(account.get("uiAmount") or 0)
        for account in holders[:10]
    )

    return (top10_amount / supply) * 100


# --------------------------------------------------
# TOKEN ANALİZİ
# --------------------------------------------------

async def evaluate(session, token):
    flags = []
    warnings = []
    data_missing = []

    mint = token.get("mint")
    dev = token.get("traderPublicKey")
    bonding_curve = token.get("bondingCurveKey")

    if not mint or not dev:
        return "VERİ EKSİK", ["Token veya dev adresi yok"], {}

    # 1. Dev ilk alım oranı
    initial_buy = token.get("initialBuy")

    dev_pct = None

    try:
        if initial_buy is not None:
            supply = await get_supply(session, mint)

            if supply and supply > 0:
                dev_pct = (
                    float(initial_buy) / supply
                ) * 100

                if dev_pct > MAX_DEV_BUY_PCT:
                    flags.append(
                        f"Dev ilk alımı yüksek: %{dev_pct:.2f}"
                    )
            else:
                data_missing.append("Token arzı alınamadı")
        else:
            data_missing.append("Dev ilk alım verisi yok")

    except Exception as error:
        print("İlk alım kontrolü:", error)
        data_missing.append("İlk alım kontrolü başarısız")

    # 2. Dev cüzdan geçmişi
    try:
        tx_count = await dev_wallet_tx_count(
            session,
            dev
        )

        if tx_count is None:
            data_missing.append("Dev işlem geçmişi alınamadı")
        elif tx_count < MIN_DEV_WALLET_TX:
            flags.append(
                f"Dev cüzdanı çok yeni: {tx_count} işlem"
            )

    except Exception as error:
        print("Dev geçmişi hatası:", error)
        tx_count = None
        data_missing.append("Dev geçmişi kontrol edilemedi")

    # 3. Sosyal bağlantılar
    metadata = await fetch_metadata(
        session,
        token.get("uri", "")
    )

    socials = {
        key: metadata.get(key)
        for key in ("twitter", "telegram", "website")
        if metadata.get(key)
    }

    if not socials:
        warnings.append("Sosyal bağlantı bulunamadı")

        if REQUIRE_SOCIALS:
            flags.append("Sosyal bağlantı zorunlu")

    # 4. Holder yoğunluğu
    top10 = None

    if bonding_curve:
        try:
            await asyncio.sleep(CHECK_DELAY_SEC)

            top10 = await top10_account_percent(
                session,
                mint,
                bonding_curve
            )

            if top10 is None:
                data_missing.append(
                    "Holder hesapları alınamadı"
                )

            elif top10 > MAX_TOP10_ACCOUNTS_PCT:
                flags.append(
                    f"İlk 10 hesap yoğunluğu yüksek: %{top10:.2f}"
                )

        except Exception as error:
            print("Holder kontrolü:", error)
            data_missing.append("Holder kontrolü başarısız")

    else:
        data_missing.append("Bonding curve adresi yok")

    info = {
        "dev_pct": dev_pct,
        "tx_count": tx_count,
        "top10": top10,
        "socials": socials,
        "warnings": warnings,
        "data_missing": data_missing
    }

    # Eksik veri varsa GEÇTİ demiyoruz.
    if data_missing:
        return "VERİ EKSİK", flags + data_missing, info

    if flags:
        return "ŞÜPHELİ", flags, info

    return "GEÇTİ", warnings, info


# --------------------------------------------------
# TELEGRAM
# --------------------------------------------------

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
        timeout=aiohttp.ClientTimeout(total=15)
    ) as response:

        if response.status != 200:
            error_text = await response.text()
            raise RuntimeError(
                f"Telegram hatası: {error_text}"
            )


def format_message(token, verdict, flags, info):
    mint = token["mint"]

    name = html.escape(str(token.get("name") or "Bilinmiyor"))
    symbol = html.escape(str(token.get("symbol") or "?"))

    dev_pct = (
        f"%{info['dev_pct']:.2f}"
        if info["dev_pct"] is not None
        else "Bilinmiyor"
    )

    tx_count = (
        str(info["tx_count"])
        if info["tx_count"] is not None
        else "Bilinmiyor"
    )

    top10 = (
        f"%{info['top10']:.2f}"
        if info["top10"] is not None
        else "Bilinmiyor"
    )

    emoji = "✅" if verdict == "GEÇTİ" else "⚠️"

    lines = [
        f"{emoji} <b>{verdict}</b>",
        f"<b>{name} (${symbol})</b>",
        "",
        f"Dev ilk alım: {dev_pct}",
        f"Dev son 50 imza sayısı: {tx_count}",
        f"İlk 10 token hesabı yoğunluğu: {top10}",
        f"Market cap: {token.get('marketCapSol', '?')} SOL",
    ]

    if flags:
        lines += [
            "",
            "<b>Risk / veri uyarıları:</b>",
            html.escape("; ".join(flags))
        ]

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
        f"https://pump.fun/coin/{mint}",
        f"https://dexscreener.com/solana/{mint}",
        f"https://rugcheck.xyz/tokens/{mint}"
    ]

    return "\n".join(lines)


# --------------------------------------------------
# CSV KAYDI
# --------------------------------------------------

def log_token(token, verdict, flags, info):
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
                    "dev_pct",
                    "dev_tx_count",
                    "top10_account_pct",
                    "verdict",
                    "flags"
                ])

            writer.writerow([
                int(time.time()),
                token.get("mint"),
                token.get("symbol"),
                token.get("marketCapSol"),
                info.get("dev_pct"),
                info.get("tx_count"),
                info.get("top10"),
                verdict,
                "; ".join(flags)
            ])

    except Exception as error:
        print("CSV kayıt hatası:", error)


# --------------------------------------------------
# TOKEN İŞLEME
# --------------------------------------------------

async def handle_token(session, token):
    mint = token.get("mint")

    if not mint:
        return

    async with semaphore:
        try:
            verdict, flags, info = await evaluate(
                session,
                token
            )

            log_token(
                token,
                verdict,
                flags,
                info
            )

            print(
                f"{token.get('symbol', '?')}: {verdict} "
                f"| Uyarı: {len(flags)}"
            )

            if verdict == "GEÇTİ" or SEND_SUSPICIOUS:
                message = format_message(
                    token,
                    verdict,
                    flags,
                    info
                )

                await send_telegram(
                    session,
                    message
                )

                print(
                    f"{token.get('symbol', '?')}: "
                    "Telegram bildirimi gönderildi."
                )

        except Exception as error:
            print(
                f"Token işleme hatası ({mint}): {error}"
            )


# --------------------------------------------------
# PUMP.FUN DİNLEME
# --------------------------------------------------

async def main():
    timeout = aiohttp.ClientTimeout(total=20)

    connector = aiohttp.TCPConnector(
        limit=20,
        limit_per_host=10
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

                    await ws.send(json.dumps({
                        "method": "subscribeNewToken"
                    }))

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
                                    "yeni token atlandı."
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
