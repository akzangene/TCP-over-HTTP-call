# session.py
import asyncio
import base64
import aiohttp
from buffer import DataBuffer
from config import Config
import json
import re

class ProxySession:
    def __init__(self, session_id: str, target_host: str, target_port: int):
        self.session_id = session_id
        self.target_host = target_host
        self.target_port = target_port

        self.local_writer: asyncio.StreamWriter | None = None
        self.client_to_server = DataBuffer()
        self.running = True
        self.current_relay_index = 0

    def add_data(self, data: bytes):
        if data:
            self.client_to_server.add(data)


    def extract_apps_script_user_html(self, text: str) -> str | None:
        """Extract embedded user HTML from an Apps Script HTML-page response.

        Google's IFRAME_SANDBOX mode returns /exec responses wrapped in an HTML
        page that includes a goog.script.init("...") call. The first argument is
        a JS string literal (\\xNN hex escapes) containing a JSON payload with
        a ``userHtml`` field that holds the actual relay response.
        """
        marker = 'goog.script.init("'
        start = text.find(marker)
        if start == -1:
            return None
        start += len(marker)
        end = text.find('", "", undefined', start)
        if end == -1:
            return None

        encoded = text[start:end]
        try:
            # The JS string uses \xNN hex escapes and \/ for forward-slash.
            # Also unescape \\ → \ (JS double-backslash = literal backslash).
            # Order: hex first, then double-backslash, then \/ so that
            # \\/ (JS for literal-backslash + /) works correctly.
            decoded = re.sub(
                r'\\x([0-9a-fA-F]{2})',
                lambda m: chr(int(m.group(1), 16)),
                encoded,
            )
            decoded = decoded.replace("\\\\", "\\")
            decoded = decoded.replace("\\/", "/")
            payload = json.loads(decoded)
        except Exception:
            return None

        user_html = payload.get("userHtml")
        return user_html if isinstance(user_html, str) else None


    def load_relay_json(self, text: str) -> dict | None:
        """Parse a relay JSON body, handling Apps Script HTML wrappers."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            wrapped = self.extract_apps_script_user_html(text)
            if wrapped:
                data = self.load_relay_json(wrapped)
                if data is not None:
                    return data
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if not match:
                return None
            try:
                data = json.loads(match.group())
            except json.JSONDecodeError:
                return None
            return data if isinstance(data, dict) else None

    async def poll_server(self, http_session: aiohttp.ClientSession):
        print(f"[+] Session {self.session_id[:8]} → {self.target_host}:{self.target_port}")

        """Periodically send buffered data and receive response"""
        while self.running:
            try:
                await asyncio.sleep(Config.POLL_INTERVAL)

                to_send = self.client_to_server.get_up_to(Config.MAX_BUFFER_SIZE_UPLINK)
                encoded = base64.b64encode(to_send) if to_send else b''

                # Build headers
                headers = {
                    "Authorization": f"Bearer {Config.AUTH_TOKEN}",
                    "X-Session-ID": self.session_id,
                    "X-Target-Host": self.target_host,
                    "X-Target-Port": str(self.target_port),
                    "X-Max-Response-Size": str(Config.MAX_BUFFER_SIZE_DOWNLINK),
                }

                # === Relay Mode Logic ===
                url = Config.SERVER_URL
                params={}
                if Config.RELAY_MODE and Config.RELAY_URLS:
                    # Simple round-robin without storing index in config
                    url = Config.RELAY_URLS[self.current_relay_index]
                    self.current_relay_index = (self.current_relay_index + 1) % len(Config.RELAY_URLS)
                    params = {
                        "targetServer" : Config.SERVER_URL,
                        "Authorization": f"Bearer {Config.AUTH_TOKEN}",
                        "X-Session-ID": self.session_id,
                        "X-Target-Host": self.target_host,
                        "X-Target-Port": str(self.target_port),
                        "X-Max-Response-Size": str(Config.MAX_BUFFER_SIZE_DOWNLINK),
                    }

                async with http_session.post(
                    url,
                    data=encoded,
                    headers=headers,
                    params=params,
                    timeout=Config.CONNECTION_TIMEOUT
                ) as resp:
                    
                    if resp.status == 200:
                        # Handle GAS Relay Response
                        if Config.RELAY_MODE:
                            text = await resp.text()
                            try:
                                data = self.load_relay_json(text)
                                if "e" in data:
                                    print(f"GAS Relay Error: {data['e', 'm']}")
                                    await asyncio.sleep(2)
                                    continue
                                if "s" in data and "b" in data:
                                    body = data["b"]
                                    if body and self.local_writer:
                                        decoded = base64.b64decode(body)
                                        if decoded:
                                            self.local_writer.write(decoded)
                                            await self.local_writer.drain()
                            except:
                                pass  # fallback
                        else:
                            resp_data = await resp.read()
                            if resp_data and self.local_writer:
                                decoded = base64.b64decode(resp_data)
                                if decoded:
                                    self.local_writer.write(decoded)
                                    await self.local_writer.drain()
                    elif resp.status == 401:
                        print("❌ Authentication failed")
                        break
                    elif resp.status == 502:
                        print(f"❌ Cannot connect to {self.target_host}:{self.target_port}")
                        break
                    else:
                        print(f"Server returned status: {resp.status}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Polling error: {e}")
                await asyncio.sleep(1)

    async def close(self):
        self.running = False
        if self.local_writer:
            try:
                self.local_writer.close()
                await self.local_writer.wait_closed()
            except:
                pass