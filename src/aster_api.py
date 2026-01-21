from .http_client import HttpClient


class AsterAPI:
    def __init__(self, rest_base: str):
        self.http = HttpClient(rest_base)

    def exchange_info(self) -> dict:
        return self.http.get("/fapi/v1/exchangeInfo")

    def ticker_24h(self) -> list[dict]:
        return self.http.get("/fapi/v1/ticker/24hr")

    def book_ticker(self) -> list[dict]:
        return self.http.get("/fapi/v1/ticker/bookTicker")

    def klines(self, symbol: str, interval: str = "1m", limit: int = 200) -> list:
        return self.http.get("/fapi/v1/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
