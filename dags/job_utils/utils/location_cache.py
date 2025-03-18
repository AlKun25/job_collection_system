from collections import OrderedDict

class LocationCache:
    _instance = None
    MAX_SIZE = 100
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LocationCache, cls).__new__(cls)
            cls._instance.us_locations = OrderedDict()
            cls._instance.non_us_locations = OrderedDict()
        return cls._instance
    
    def add_location(self, location, is_us=True):
        # Select the appropriate cache
        cache = self.us_locations if is_us else self.non_us_locations
        
        # Check if already exists - if so, move to end (most recently used)
        if location in cache:
            cache.pop(location)
        
        # If at capacity, remove least recently used item
        if len(cache) >= self.MAX_SIZE:
            cache.popitem(last=False)
            
        # Add new location (or move existing to most recently used position)
        cache[location] = True
    
    def is_us_location(self, location):
        # Check US locations first
        if location in self.us_locations:
            self.us_locations.move_to_end(location)
            return True
        
        # Check non-US locations
        if location in self.non_us_locations:
            self.non_us_locations.move_to_end(location)
            return False
            
        # Location not found in either cache
        return None
    
    def get_all_locations(self, is_us=True):
        return list(self.us_locations.keys()) if is_us else list(self.non_us_locations.keys())