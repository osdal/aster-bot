# mirror_paper_to_live.py
import asyncio
import os
from dotenv import load_dotenv
import aiohttp
import time

# Загружаем ключи из .env
load_dotenv()
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

# Настройки бота
SYMBOL = "BTCUSDT"  # Пример пары
POSITION_SIZE = 0.01  # Размер позиции
TP_PERCENT = 0.6 / 100  # 0.6% Take Profit
SL_PERCENT = 0.2 / 100  # 0.2% Stop Loss
POLL_INTERVAL = 2  # Интервал проверки позиции в секундах

class AsterDexClient:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.asterdex.com"  # пример

    async def place_order(self, symbol, side, quantity, price=None, order_type="MARKET"):
        # Отправка ордера
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/v1/order"
            data = {
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "quantity": quantity,
            }
            if price:
                data["price"] = price
            headers = {"X-API-KEY": self.api_key}
            async with session.post(url, json=data, headers=headers) as resp:
                return await resp.json()

    async def place_tp_sl(self, symbol, side, entry_price, quantity):
        # Рассчитываем цены TP и SL
        if side == "BUY":
            tp_price = entry_price * (1 + TP_PERCENT)
            sl_price = entry_price * (1 - SL_PERCENT)
        else:
            tp_price = entry_price * (1 - TP_PERCENT)
            sl_price = entry_price * (1 + SL_PERCENT)

        # Выставляем ордера Take Profit и Stop Loss
        print(f"Placing TP at {tp_price:.2f} and SL at {sl_price:.2f}")
        await self.place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=tp_price, order_type="LIMIT")
        await self.place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=sl_price, order_type="STOP")

    async def get_open_positions(self):
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/v1/positions"
            headers = {"X-API-KEY": self.api_key}
            async with session.get(url, headers=headers) as resp:
                return await resp.json()

async def monitor_position(client, symbol):
    while True:
        positions = await client.get_open_positions()
        if not positions:
            print(f"Position for {symbol} closed")
            break
        await asyncio.sleep(POLL_INTERVAL)

async def main():
    client = AsterDexClient(API_KEY, API_SECRET)

    # Пример открытия позиции
    side = "BUY"
    quantity = POSITION_SIZE
    order_resp = await client.place_order(SYMBOL, side, quantity)
    print(f"Order response: {order_resp}")

    entry_price = float(order_resp.get("price", 0)) or 50000  # fallback цена
    await client.place_tp_sl(SYMBOL, side, entry_price, quantity)

    # Мониторим позицию до закрытия
    await monitor_position(client, SYMBOL)

if __name__ == "__main__":
    asyncio.run(main())
