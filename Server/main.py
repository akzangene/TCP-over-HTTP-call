# main.py
import asyncio
from aiohttp import web
from server import ProxyServer
from config import Config

async def main():
    proxy = ProxyServer()
    app = proxy.create_app()
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, Config.HOST, Config.PORT)
    await site.start()
    
    print(f"TCP-over-HTTP Proxy Server running on http://{Config.HOST}:{Config.PORT}")
    print("Ready to accept proxy requests.")
    
    try:
        await asyncio.Future()  # run forever
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())