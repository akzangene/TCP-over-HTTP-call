# session.py
import asyncio
import uuid
from buffer import DataBuffer
from config import Config

class ProxySession:
    def __init__(self, target_host: str, target_port: int):
        self.session_id = str(uuid.uuid4())
        self.target_host = target_host
        self.target_port = target_port
        self.target_writer: asyncio.StreamWriter | None = None
        self.client_to_target = DataBuffer()
        self.target_to_client = DataBuffer()
        self.last_activity = asyncio.get_event_loop().time()
        self.is_connected = False
        self.target_closed = False   # ← New: tracks if target closed the connection

    async def connect_to_target(self):
        try:
            reader, writer = await asyncio.open_connection(
                self.target_host, self.target_port
            )
            self.target_writer = writer
            self.is_connected = True
            # Start forwarding task
            asyncio.create_task(self._forward_from_target(reader))
            return True
        except Exception as e:
            print(f"[Session {self.session_id}] Target connection failed: {e}")
            return False

    async def _forward_from_target(self, reader: asyncio.StreamReader):
        """Forward data FROM target to client buffer"""
        try:
            while True:
                data = await reader.read(8192)
                if not data:                    # ← Target sent FIN / closed connection
                    print(f"[-] Target closed connection: {self.target_host}:{self.target_port} (Session {self.session_id[:12]})")
                    self.target_closed = True
                    break
                
                self.target_to_client.add(data)
                self.last_activity = asyncio.get_event_loop().time()
        except Exception:
            self.target_closed = True
        finally:
            self.is_connected = False

    def add_data_from_client(self, data: bytes):
        self.client_to_target.add(data)
        self.last_activity = asyncio.get_event_loop().time()

    def get_data_for_client(self) -> bytes:
        return self.target_to_client.get_all()

    async def flush_to_target(self):
        if not self.target_writer or self.client_to_target.is_empty():
            return
        data = self.client_to_target.get_all()
        try:
            self.target_writer.write(data)
            await self.target_writer.drain()
        except Exception:
            self.is_connected = False
            self.target_closed = True

    def is_expired(self) -> bool:
        # Close session faster if target already closed the connection
        if self.target_closed:
            return True
        return (asyncio.get_event_loop().time() - self.last_activity) > Config.IDLE_TIMEOUT

    async def close(self):
        if self.target_writer:
            try:
                self.target_writer.close()
                await self.target_writer.wait_closed()
            except:
                pass