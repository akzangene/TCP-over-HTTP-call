# session.py
import asyncio
import base64
import aiohttp
from buffer import DataBuffer
from config import Config

class ProxySession:
    def __init__(self, session_id: str, target_host: str, target_port: int):
        self.session_id = session_id
        self.target_host = target_host
        self.target_port = target_port

        self.local_writer: asyncio.StreamWriter | None = None
        self.client_to_server = DataBuffer()
        self.running = True

    def add_data(self, data: bytes):
        if data:
            self.client_to_server.add(data)

    async def poll_server(self, http_session: aiohttp.ClientSession):
        print(f"[+] Session {self.session_id[:8]} → {self.target_host}:{self.target_port}")

        """Periodically send buffered data and receive response"""
        while self.running:
            try:
                await asyncio.sleep(Config.POLL_INTERVAL)

                # === Limit upload size per request ===
                all_data = self.client_to_server.get_all()   # This clears the buffer

                if len(all_data) > Config.MAX_BUFFER_SIZE_UPLINK:
                    to_send = all_data[:Config.MAX_BUFFER_SIZE_UPLINK]
                    remaining = all_data[Config.MAX_BUFFER_SIZE_UPLINK:]
                    if remaining:
                        self.client_to_server.buffer.appendleft(remaining)
                        self.client_to_server.total_size = len(remaining)
                else:
                    to_send = all_data

                encoded = base64.b64encode(to_send) if to_send else b''

                headers = {
                    "Authorization": f"Bearer {Config.AUTH_TOKEN}",
                    "X-Session-ID": self.session_id,
                    "X-Target-Host": self.target_host,
                    "X-Target-Port": str(self.target_port),
                    "X-Max-Response-Size": str(Config.MAX_BUFFER_SIZE_DOWNLINK),
                }

                async with http_session.post(
                    Config.SERVER_URL,
                    data=encoded,
                    headers=headers,
                    timeout=Config.CONNECTION_TIMEOUT
                ) as resp:
                    
                    if resp.status == 200:
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