# server.py
from aiohttp import web
import asyncio
from session import ProxySession
from config import Config
import base64
import struct

class ProxyServer:
    def __init__(self):
        self.sessions: dict[str, ProxySession] = {}
        self.cleanup_task = None

    async def start_cleanup_task(self):
        self.cleanup_task = asyncio.create_task(self._cleanup_expired_sessions())

    async def _cleanup_expired_sessions(self):
        while True:
            await asyncio.sleep(30)
            expired = [sid for sid, session in self.sessions.items() if session.is_expired()]
            for sid in expired:
                await self.sessions[sid].close()
                del self.sessions[sid]
            if len(self.sessions) > Config.MAX_SESSIONS * 0.8:
                print(f"Warning: High session count: {len(self.sessions)}")

    async def handle_batch(self, request: web.Request):

        # Simple auth
        token = request.headers.get("Authorization")
        if token != f"Bearer {Config.AUTH_TOKEN}":
            return web.Response(status=401, text="Unauthorized")

        remaining_downlink = int(request.headers.get("X-Max-Response-Size"))

        encoded_body = await request.read()

        body = base64.b64decode(
            encoded_body
        )

        offset = 0

        count = struct.unpack_from(
            "!I",
            body,
            offset
        )[0]

        offset += 4

        response = bytearray()

        response += struct.pack("!I", 0)

        response_count = 0

        for _ in range(count):

            try:

                sid_len = struct.unpack_from(
                    "!H",
                    body,
                    offset
                )[0]

                offset += 2

                session_id = body[
                    offset:offset + sid_len
                ].decode()

                offset += sid_len

                host_len = struct.unpack_from(
                    "!H",
                    body,
                    offset
                )[0]

                offset += 2

                target_host = body[
                    offset:offset + host_len
                ].decode()

                offset += host_len

                target_port = struct.unpack_from(
                    "!H",
                    body,
                    offset
                )[0]

                offset += 2

                payload_len = struct.unpack_from(
                    "!I",
                    body,
                    offset
                )[0]

                offset += 4

                payload = body[offset:offset + payload_len]
                offset += payload_len

                # 1. Non-blocking Session Initialization
                if session_id not in self.sessions:
                    if len(self.sessions) >= Config.MAX_SESSIONS:
                        return web.Response(status=503, text="Too many sessions")
            
                    session = ProxySession(target_host, target_port)
                    self.sessions[session_id] = session
                    
                    # 🔥 NON-BLOCKING: Fire connection off to background
                    session.start_connection()

                session = self.sessions[session_id]

                # 2. Drop dead sessions early
                if session.connection_failed:
                    continue

                # 3. Queue data instantly (Flushing happens on a background task)
                if payload:
                    session.add_data_from_client(payload)

                # 4. Grab whatever is currently available in the buffer
                response_data, consumed = session.get_data_for_client_up_to(remaining_downlink)

                if not response_data:
                    continue

                sid = session_id.encode()

                item = bytearray()
                item += struct.pack("!H", len(sid))
                item += sid
                item += struct.pack("!I", len(response_data))
                item += response_data
                response += item
                response_count += 1
                remaining_downlink -= consumed

            except Exception as e:
                print("batch item error", e)

        struct.pack_into(
            "!I",
            response,
            0,
            response_count
        )

        encoded_response = base64.b64encode(
            bytes(response)
        )

        resp = web.Response(
            body=encoded_response
        )
        
        resp.enable_compression()
        
        return resp

    def create_app(self):
        app = web.Application()
        app.router.add_post("/proxy", self.handle_batch)
        
        async def on_startup(app):
            await self.start_cleanup_task()
        
        app.on_startup.append(on_startup)
        return app