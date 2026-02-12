# mirror_paper_to_live.py
import os
import requests
import time
from dotenv import load_dotenv

# Загружаем ключи из .env
load_dotenv()
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

# Настройки бота
SYMBOL = "BTCUSDT"
POSITION_SIZE = 0.01
TP_PERCENT = 0.6 / 100  # 0.6% Take Profit
SL_PERCENT = 0.2 / 100  # 0.2% Stop Loss
POLL_INTERVAL = 2  # Проверка позиции каждые 2 секунды

class AsterDexClient:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.asterdex.com"  # пример

    def place_order(self, symbol, side, quantity, price=None, order_type="MARKET"):
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
        resp = requests.post(url, json=data, headers=headers)
        return resp.json()

    def place_tp_sl(self, symbol, side, entry_price, quantity):
        if side == "BUY":
            tp_price = entry_price * (1 + TP_PERCENT)
            sl_price = entry_price * (1 - SL_PERCENT)
        else:
            tp_price = entry_price * (1 - TP_PERCENT)
            sl_price = entry_price * (1 + SL_PERCENT)

        print(f"Placing TP at {tp_price:.2f} and SL at {sl_price:.2f}")
        # Take Profit
        self.place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=tp_price, order_type="LIMIT")
        # Stop Loss
        self.place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=sl_price, order_type="STOP")

    def get_open_positions(self):
        url = f"{self.base_url}/v1/positions"
        headers = {"X-API-KEY": self.api_key}
        resp = requests.get(url, headers=headers)
        return resp.json()

def monitor_position(client, symbol):
    while True:
        positions = client.get_open_positions()
        if not positions:
            print(f"Position for {symbol} closed")
            break
        time.sleep(POLL_INTERVAL)

def main():
    client = AsterDexClient(API_KEY, API_SECRET)

    # Открываем позицию
    side = "BUY"
    quantity = POSITION_SIZE
    order_resp = client.place_order(SYMBOL, side, quantity)
    print(f"Order response: {order_resp}")

    entry_price = float(order_resp.get("price", 0)) or 50000  # fallback цена
    client.place_tp_sl(SYMBOL, side, entry_price, quantity)

    # Мониторим позицию до закрытия
    monitor_position(client, SYMBOL)

if __name__ == "__main__":
    main()
