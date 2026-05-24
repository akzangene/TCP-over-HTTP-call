# config.py
import os

class Config:
    SERVER_URL = "http://127.0.0.1:8080/proxy"
    LOCAL_HOST = "0.0.0.0"
    LOCAL_PORT = 1080                            # Default local listening port

    # Buffering & timing
    MAX_BUFFER_SIZE_UPLINK = 2 * 128 * 1024
    MAX_BUFFER_SIZE_DOWNLINK = 2 * 2 * 1024 * 1024
    POLL_INTERVAL = 2

    READ_CHUNK_SIZE = 8192
    CONNECTION_TIMEOUT = 25

    # Security
    AUTH_TOKEN = "change_me_in_production"