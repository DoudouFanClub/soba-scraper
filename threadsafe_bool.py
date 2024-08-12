import threading

class Threadsafe_Boolean:
    def __init__(self, initial=False):
        self.event = threading.Event()
        self.disable()
        if initial:
            self.event.set()

    def enable(self):
        self.event.set()

    def disable(self):
        self.event.clear()
    
    def wait_for_completion(self):
        self.event.wait()