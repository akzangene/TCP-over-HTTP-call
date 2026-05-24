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

    def get_up_to(self, max_size: int) -> bytes:
        """Return up to max_size bytes without removing them from buffer if not fully consumed"""
        if not self.buffer or max_size <= 0:
            return b''

        if self.total_size <= max_size:
            return self.get_all()  # Use existing method for full consumption

        # Need to take only part of the data
        data_list = []
        remaining = max_size

        while self.buffer and remaining > 0:
            chunk = self.buffer.popleft()
            if len(chunk) <= remaining:
                data_list.append(chunk)
                remaining -= len(chunk)
            else:
                # Split chunk
                to_take = chunk[:remaining]
                leftover = chunk[remaining:]
                data_list.append(to_take)
                self.buffer.appendleft(leftover)
                remaining = 0

        taken_data = b''.join(data_list)
        self.total_size -= len(taken_data)
        self.last_flush = time.time()
        return taken_data