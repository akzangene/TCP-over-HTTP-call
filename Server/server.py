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

    async def handle_proxy(self, request: web.Request):
        if request.method != "POST":
            return web.Response(status=405)

        # Simple auth
        token = request.headers.get("Authorization")
        if token != f"Bearer {Config.AUTH_TOKEN}":
            return web.Response(status=401, text="Unauthorized")

        session_id = request.headers.get("X-Session-ID")
        target_host = request.headers.get("X-Target-Host")
        target_port = request.headers.get("X-Target-Port")
        max_response = int(request.headers.get("X-Max-Response-Size"))

        if not session_id or not target_host or not target_port:
            return web.Response(status=400, text="Missing headers")

        try:
            target_port = int(target_port)
        except ValueError:
            return web.Response(status=400, text="Invalid port")

        # Get or create session
        if session_id not in self.sessions:
            if len(self.sessions) >= Config.MAX_SESSIONS:
                return web.Response(status=503, text="Too many sessions")
            
            session = ProxySession(target_host, target_port)
            if not await session.connect_to_target():
                return web.Response(status=502, text="Cannot connect to target")
            self.sessions[session_id] = session

        session = self.sessions[session_id]

        # Read data from client
        body = await request.read()
        if body:
            try:
                decoded = base64.b64decode(body)
                session.add_data_from_client(decoded)
                await session.flush_to_target()
            except:
                pass

        # Limit response size as requested by client
        response_data = session.get_data_for_client()[:max_response]

        encoded = base64.b64encode(response_data)

        return web.Response(
            body=encoded,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Session-Active": "true"
            }
        )

    def create_app(self):
        app = web.Application()
        app.router.add_post("/proxy", self.handle_proxy)
        
        async def on_startup(app):
            await self.start_cleanup_task()
        
        app.on_startup.append(on_startup)
        return app