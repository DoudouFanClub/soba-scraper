import threading

class Threadsafe_Set:
    def __init__(self):
        self._set = set()
        self._lock = threading.Lock()

    def add(self, item):
        with self._lock:
            self._set.add(item)

    def contains(self, item):
        with self._lock:
            return item in self._set
        
    def clear(self):
        with self._lock:
            self._set.clear()
    
    def get_copy(self):
        with self._lock:
            return frozenset(self._set)