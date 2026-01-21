import os, time, hmac, hashlib, urllib.parse, urllib.request, json
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
KEY = os.getenv("ASTER_API_KEY")
SEC = os.getenv("ASTER_API_SECRET")
SYM = os.getenv("LIVE_SYMBOL", "ASTERUSDT")

params = {
    "symbol": SYM,
    "timestamp": str(int(time.time() * 1000)),
    "recvWindow": "5000",
}
q = urllib.parse.urlencode(params)
sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
url = f"{BASE}/fapi/v1/openOrders?{q}&signature={sig}"

req = urllib.request.Request(url, headers={"X-MBX-APIKEY": KEY})
with urllib.request.urlopen(req, timeout=20) as resp:
    data = json.loads(resp.read().decode("utf-8"))
print(json.dumps(data, ensure_ascii=False, indent=2))
