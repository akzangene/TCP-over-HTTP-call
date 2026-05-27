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
        self.current_relay_index = 0

    def add_data(self, data: bytes):
        if data:
            self.client_to_server.add(data)

    async def close(self):
        self.running = False
        if self.local_writer:
            try:
                self.local_writer.close()
                await self.local_writer.wait_closed()
            except:
                pass