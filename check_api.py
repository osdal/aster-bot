import os, time, hmac, hashlib, urllib.parse, urllib.request, json

# optional .env support
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

path = "/fapi/v4/account"  # Account Information V4 (USER_DATA)
ts = int(time.time() * 1000)
params = {
    "recvWindow": 5000,
    "timestamp": ts,
}
query = urllib.parse.urlencode(params)
sig = hmac.new(API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
url = f"{BASE}{path}?{query}&signature={sig}"

req = urllib.request.Request(url, method="GET", headers={"X-MBX-APIKEY": API_KEY})
try:
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = resp.read().decode("utf-8")
        print("OK. HTTP", resp.status)
        # печатаем кратко, без простыни
        obj = json.loads(data)
        print("assets:", len(obj.get("assets", [])))
        print("positions:", len(obj.get("positions", [])))
        # покажем первые 3 assets (если есть)
        for a in (obj.get("assets", [])[:3]):
            print("asset:", a.get("asset"), "walletBalance:", a.get("walletBalance"), "availableBalance:", a.get("availableBalance"))
except Exception as e:
    print("ERROR:", e)
    raise
