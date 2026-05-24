# main.py
import asyncio
from local_server import LocalProxyServer

async def main():
    proxy = LocalProxyServer()
    await proxy.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProxy stopped.")