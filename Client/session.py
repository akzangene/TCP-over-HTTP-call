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

    async def close(self):
        self.running = False
        if self.local_writer:
            try:
                self.local_writer.close()
                await self.local_writer.wait_closed()
            except:
                pass