
class Cache:

    def __init__(self):
        self.cache = {}

    def get(self, k):
        return self.cache.get(k, None)

    def put(self, k, v):
        self.cache[k] = v
