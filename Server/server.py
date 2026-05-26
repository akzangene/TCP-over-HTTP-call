# server.py
from aiohttp import web
import asyncio
from session import ProxySession
from config import Config
import base64

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

        #1 max_response_size = int(request.headers.get("X-Max-Response-Size"))

        payload = await request.json()
        result = []
        for item in payload.get("sessions", []):
            try:
                session_id = item["session_id"]
                target_host = item["target_host"]
                target_port = int(item["target_port"])

                # Get or create session
                if session_id not in self.sessions:
                    if len(self.sessions) >= Config.MAX_SESSIONS:
                        return web.Response(status=503, text="Too many sessions")
            
                    session = ProxySession(target_host, target_port)
                    ok = await session.connect_to_target()
                    if not ok:
                        #2 return 502
                        continue
                    self.sessions[session_id] = session

                session = self.sessions[session_id]

                body = item.get("data", "")
                if body:
                    decoded = base64.b64decode(body)
                    if decoded:
                        session.add_data_from_client(decoded)
                        await session.flush_to_target()
                response_data = session.get_data_for_client()
                result.append({
                    "session_id": session_id,
                    "data": base64.b64encode(response_data).decode(),
                })

            except Exception as e:
                print("batch item error", e)

        return web.json_response({
            "sessions": result
        })

    def create_app(self):
        app = web.Application()
        app.router.add_post("/proxy", self.handle_batch)
        
        async def on_startup(app):
            await self.start_cleanup_task()
        
        app.on_startup.append(on_startup)
        return app