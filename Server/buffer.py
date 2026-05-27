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

    def get_up_to(self, max_size: int) -> tuple[bytes, int]:

        if not self.buffer or max_size <= 0:
            return b'', 0

        if self.total_size <= max_size:
            data = self.get_all()
            return data, len(data)

        data_list = []
        remaining = max_size

        while self.buffer and remaining > 0:

            chunk = self.buffer.popleft()

            if len(chunk) <= remaining:

                data_list.append(chunk)

                remaining -= len(chunk)

            else:

                take = chunk[:remaining]
                leftover = chunk[remaining:]

                data_list.append(take)

                self.buffer.appendleft(leftover)

                remaining = 0

        data = b''.join(data_list)

        consumed = len(data)

        self.total_size -= consumed

        self.last_flush = time.time()

        return data, consumed

    def should_flush(self, max_size: int, interval: float) -> bool:
        if self.total_size >= max_size:
            return True
        return (time.time() - self.last_flush) >= interval

    def is_empty(self) -> bool:
        return self.total_size == 0