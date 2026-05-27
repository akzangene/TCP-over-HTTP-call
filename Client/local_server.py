# local_server.py
import asyncio
import aiohttp
import uuid
import struct
from session import ProxySession
from config import Config
import base64

class LocalProxyServer:
    def __init__(self):
        self.active_sessions = {}

    async def handle_socks(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        session_id = str(uuid.uuid4())[:12]
        addr = writer.get_extra_info('peername')

        try:
            # SOCKS5 Greeting
            data = await reader.read(512)
            if data[0] != 5:
                writer.close()
                return

            writer.write(b'\x05\x00')  # No authentication
            await writer.drain()

            # SOCKS5 Request
            data = await reader.read(512)
            if data[1] != 1:  # Only CONNECT supported
                writer.write(b'\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00')
                await writer.drain()
                return

            # Parse destination
            atyp = data[3]
            if atyp == 1:   # IPv4
                host = '.'.join(map(str, data[4:8]))
                port = struct.unpack('>H', data[8:10])[0]
            elif atyp == 3: # Domain
                length = data[4]
                host = data[5:5+length].decode()
                port = struct.unpack('>H', data[5+length:7+length])[0]
            else:
                writer.write(b'\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00')
                await writer.drain()
                return

            # Success response
            writer.write(b'\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()

            # Create session
            session = ProxySession(session_id, host, port)
            session.local_writer = writer
            self.active_sessions[session_id] = session

        # Start polling task
            try:
                while True:
                    data = await reader.read(Config.READ_CHUNK_SIZE)
                    if not data:
                        break
                    session.add_data(data)
            finally:
                await session.close()
                self.active_sessions.pop(session_id, None)

        except Exception as e:
            print(f"SOCKS error: {e}")
        finally:
            writer.close()

    async def start(self):
        asyncio.create_task(self.global_poll_loop())
        server = await asyncio.start_server(
            self.handle_socks, Config.LOCAL_HOST, Config.LOCAL_PORT
        )
        addr = server.sockets[0].getsockname()
        print(f"🚀 SOCKS5 Proxy listening on {addr} (connect your phone here)")
        print(f"→ HTTP Tunnel: {Config.SERVER_URL}\n")

        async with server:
            await server.serve_forever()

    async def global_poll_loop(self):

        async with aiohttp.ClientSession() as http_session:

            while True:

                await asyncio.sleep(Config.POLL_INTERVAL)

                frame = bytearray()

                frame += struct.pack("!I", 0)

                session_count = 0

                remaining_budget = Config.MAX_BUFFER_SIZE_UPLINK

                for session_id, session in list(self.active_sessions.items()):

                    outgoing, consumed = session.client_to_server.get_up_to(remaining_budget)

                    sid = session_id.encode()
                    host = session.target_host.encode()

                    item = bytearray()

                    item += struct.pack("!H", len(sid))
                    item += sid

                    item += struct.pack("!H", len(host))
                    item += host

                    item += struct.pack(
                        "!H",
                        session.target_port
                    )

                    item += struct.pack(
                        "!I",
                        len(outgoing)
                    )

                    item += outgoing

                    frame += item

                    session_count += 1

                    remaining_budget -= consumed

                struct.pack_into(
                    "!I",
                    frame,
                    0,
                    session_count
                )

                encoded_frame = base64.b64encode(bytes(frame))

                print(f"ONE REQUEST -> {session_count} sessions")

                try:

                    async with http_session.post(
                        Config.SERVER_URL,
                        data=encoded_frame,
                        headers={
                            "Authorization": f"Bearer {Config.AUTH_TOKEN}",
                            "X-Max-Response-Size": str(Config.MAX_BUFFER_SIZE_DOWNLINK)
                        },
                        timeout=Config.CONNECTION_TIMEOUT,
                    ) as resp:

                        if resp.status != 200:
                            print("batch failed", resp.status)
                            continue

                        encoded_response = await resp.read()

                        body = base64.b64decode(encoded_response)

                        offset = 0

                        count = struct.unpack_from(
                            "!I",
                            body,
                            offset
                        )[0]

                        offset += 4

                        for _ in range(count):

                            sid_len = struct.unpack_from(
                                "!H",
                                body,
                                offset
                            )[0]

                            offset += 2

                            sid = body[
                                offset:offset + sid_len
                            ].decode()

                            offset += sid_len

                            payload_len = struct.unpack_from(
                                "!I",
                                body,
                                offset
                            )[0]

                            offset += 4

                            payload = body[
                                offset:offset + payload_len
                            ]

                            offset += payload_len

                            if sid not in self.active_sessions:
                                continue

                            session = self.active_sessions[sid]

                            if payload and session.local_writer:
                                session.local_writer.write(payload)
                                await session.local_writer.drain()

                except Exception as e:
                    print("global poll error", e)