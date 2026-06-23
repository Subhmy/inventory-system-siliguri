"""
In-Memory Cache for Fast Data Access
Data is loaded once and served from memory
"""

import time
import threading
from datetime import datetime, timedelta
import json

class DataCache:
    """Simple in-memory cache for MongoDB data"""
    
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = {}  # Time to live in seconds
        self._lock = threading.Lock()
        
        # Default cache TTL (5 minutes)
        self.default_ttl = 300
    
    def get(self, key):
        """Get cached data if not expired"""
        with self._lock:
            if key in self._cache and key in self._cache_time:
                # Check if expired
                ttl = self._cache_ttl.get(key, self.default_ttl)
                if datetime.now() - self._cache_time[key] < timedelta(seconds=ttl):
                    print(f"📦 Cache HIT: {key}")
                    return self._cache[key]
                else:
                    print(f"⏰ Cache EXPIRED: {key}")
            else:
                print(f"❌ Cache MISS: {key}")
            return None
    
    def set(self, key, data, ttl=None):
        """Set cached data with optional TTL"""
        with self._lock:
            self._cache[key] = data
            self._cache_time[key] = datetime.now()
            self._cache_ttl[key] = ttl or self.default_ttl
            print(f"💾 Cache SET: {key} (TTL: {self._cache_ttl[key]}s)")
    
    def clear(self, key=None):
        """Clear specific cache or all cache"""
        with self._lock:
            if key:
                self._cache.pop(key, None)
                self._cache_time.pop(key, None)
                self._cache_ttl.pop(key, None)
                print(f"🗑️ Cache CLEAR: {key}")
            else:
                self._cache.clear()
                self._cache_time.clear()
                self._cache_ttl.clear()
                print("🗑️ Cache CLEAR: All")
    
    def get_stats(self):
        """Get cache statistics"""
        with self._lock:
            return {
                'total_items': len(self._cache),
                'keys': list(self._cache.keys())
            }

# Singleton cache instance
_cache = DataCache()

def get_cache():
    """Get the global cache instance"""
    return _cache

def cached(ttl=300):
    """Decorator to cache function results"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            key = f"{func.__name__}_{args}_{kwargs}"
            cache = get_cache()
            
            # Try to get from cache
            result = cache.get(key)
            if result is not None:
                return result
            
            # Not in cache, execute function
            result = func(*args, **kwargs)
            
            # Store in cache
            cache.set(key, result, ttl)
            return result
        return wrapper
    return decorator