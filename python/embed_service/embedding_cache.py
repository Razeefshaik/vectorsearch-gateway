"""A bounded, process-local cache for deterministic embeddings."""

from collections import OrderedDict
from hashlib import sha256
from threading import Lock


class EmbeddingCache:
    def __init__(self, max_entries: int):
        if max_entries < 0:
            raise ValueError("max_entries must not be negative")
        self.max_entries = max_entries
        self._entries = OrderedDict()
        self._lock = Lock()

    @staticmethod
    def _key(model_name: str, text: str):
        return model_name, sha256(text.encode("utf-8")).digest()

    def get(self, model_name: str, text: str):
        if self.max_entries == 0:
            return None
        key = self._key(model_name, text)
        with self._lock:
            value = self._entries.get(key)
            if value is not None:
                self._entries.move_to_end(key)
            return value

    def put(self, model_name: str, text: str, vector):
        if self.max_entries == 0:
            return
        key = self._key(model_name, text)
        with self._lock:
            self._entries[key] = tuple(vector)
            self._entries.move_to_end(key)
            if len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
