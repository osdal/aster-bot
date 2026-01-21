import json
import threading
import time
from typing import Callable, Optional

import websocket


class WSClient:
    def __init__(self, ws_base: str, streams: list[str], on_message: Callable[[dict], None]):
        self.ws_base = ws_base.rstrip("/")
        self.streams = streams
        self.on_message_cb = on_message

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def _build_url(self) -> str:
        streams_path = "/".join(self.streams)
        return f"{self.ws_base}/stream?streams={streams_path}"

    def start(self) -> None:
        url = self._build_url()

        def _on_open(ws):
            print(f"[WS] connected: {url}")

        def _on_close(ws, code, msg):
            print(f"[WS] closed: code={code} msg={msg}")

        def _on_error(ws, err):
            print(f"[WS] error: {err}")

        def _on_message(ws, msg: str):
            try:
                data = json.loads(msg)
            except Exception:
                return
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
                self.on_message_cb(data)
            elif isinstance(data, dict):
                self.on_message_cb({"stream": None, "data": data})

        def _run():
            while not self._stop.is_set():
                try:
                    self._ws = websocket.WebSocketApp(
                        url,
                        on_open=_on_open,
                        on_message=_on_message,
                        on_error=_on_error,
                        on_close=_on_close,
                    )
                    self._ws.run_forever(ping_interval=20, ping_timeout=10)
                except Exception as e:
                    print(f"[WS] run exception: {e}")

                if not self._stop.is_set():
                    time.sleep(2)

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=3)
