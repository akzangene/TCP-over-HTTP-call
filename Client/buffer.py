# buffer.py
import time
from collections import deque

class DataBuffer:
    def __init__(self):
        self.buffer = deque()
        self.last_flush = time.time()
        self.total_size = 0

    def add(self, data: bytes):
        if not data:
            return
        self.buffer.append(data)
        self.total_size += len(data)

    def get_all(self) -> bytes:
        if not self.buffer:
            return b''
        data = b''.join(self.buffer)
        self.buffer.clear()
        self.total_size = 0
        self.last_flush = time.time()
        return data

    def is_empty(self) -> bool:
        return self.total_size == 0