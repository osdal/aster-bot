import os, time, hmac, hashlib, urllib.parse, urllib.request, json
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")
SYMBOL = os.getenv("LIVE_SYMBOL", "ASTERUSDT")
ORDER_ID = os.getenv("CHECK_ORDER_ID", "2304734695")

def sign(params: dict) -> str:
    query = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return query + "&signature=" + sig

params = {
    "symbol": SYMBOL,
    "orderId": ORDER_ID,
    "timestamp": str(int(time.time() * 1000)),
    "recvWindow": "5000",
}
url = f"{BASE}/fapi/v1/order?{sign(params)}"
req = urllib.request.Request(url, method="GET", headers={"X-MBX-APIKEY": API_KEY})
with urllib.request.urlopen(req, timeout=20) as resp:
    obj = json.loads(resp.read().decode("utf-8"))
    print(json.dumps(obj, ensure_ascii=False, indent=2))
