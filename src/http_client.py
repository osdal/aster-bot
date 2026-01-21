import requests


class HttpClient:
    def __init__(self, base_url: str, timeout_sec: int = 15):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    def get(self, path: str, params: dict | None = None) -> dict | list:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, params=params, timeout=self.timeout_sec)
        r.raise_for_status()
        return r.json()
