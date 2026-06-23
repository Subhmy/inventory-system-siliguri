"""
MongoDB Database Helper for IMS Siliguri
Supports both siliguri_electrical (inventory data) and ims_siliguri (user data)
Enhanced with connection pooling, caching, and performance optimizations
Last Updated: June 23, 2026 - FIXED: TLS connection conflict on Render
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure, InvalidURI
from datetime import datetime, timedelta
import os
import time
import hashlib
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ================================================================
# CONFIGURATION
# ================================================================
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'siliguri_electrical')

# Inventory database (where all inventory data lives)
INVENTORY_DB_NAME = os.getenv('INVENTORY_DB_NAME', 'siliguri_electrical')

# User database (where users collection lives)
USER_DB_NAME = os.getenv('USER_DB_NAME', 'ims_siliguri')

# Connection Pool Settings - Optimized for performance
MAX_POOL_SIZE = int(os.getenv('MONGO_MAX_POOL_SIZE', '50'))
MIN_POOL_SIZE = int(os.getenv('MONGO_MIN_POOL_SIZE', '10'))
MAX_IDLE_TIME_MS = int(os.getenv('MONGO_MAX_IDLE_TIME_MS', '10000'))
CONNECT_TIMEOUT_MS = int(os.getenv('MONGO_CONNECT_TIMEOUT_MS', '20000'))
SOCKET_TIMEOUT_MS = int(os.getenv('MONGO_SOCKET_TIMEOUT_MS', '20000'))
SERVER_SELECTION_TIMEOUT_MS = int(os.getenv('MONGO_SERVER_SELECTION_TIMEOUT_MS', '30000'))

# Retry Settings
MAX_RETRIES = int(os.getenv('MONGO_MAX_RETRIES', '3'))
RETRY_DELAY_SECONDS = int(os.getenv('MONGO_RETRY_DELAY_SECONDS', '2'))

# Cache Settings
CACHE_ENABLED = os.getenv('MONGO_CACHE_ENABLED', 'true').lower() == 'true'
CACHE_TTL = int(os.getenv('MONGO_CACHE_TTL', '300'))  # 5 minutes default

# Detect environment
IS_RENDER = os.getenv('RENDER', 'false').lower() == 'true'
IS_LOCAL = 'localhost' in MONGO_URI or '127.0.0.1' in MONGO_URI

# ================================================================
# GLOBAL STATE
# ================================================================
_client = None
_default_db = None
_inventory_db = None
_user_db = None

_connection_status = {
    'connected': False,
    'last_attempt': None,
    'last_success': None,
    'error_count': 0,
    'last_error': None
}

# ================================================================
# IN-MEMORY CACHE SYSTEM
# ================================================================

class MongoDBQueryCache:
    """Cache for MongoDB queries to reduce database load"""
    
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = {}
        self.default_ttl = CACHE_TTL
        self._hit_count = 0
        self._miss_count = 0
    
    def get(self, key):
        """Get cached query result if not expired"""
        if key in self._cache and key in self._cache_time:
            ttl = self._cache_ttl.get(key, self.default_ttl)
            if datetime.now() - self._cache_time[key] < timedelta(seconds=ttl):
                self._hit_count += 1
                return self._cache[key]
        self._miss_count += 1
        return None
    
    def set(self, key, data, ttl=None):
        """Cache query result with TTL"""
        self._cache[key] = data
        self._cache_time[key] = datetime.now()
        self._cache_ttl[key] = ttl or self.default_ttl
    
    def clear(self, key=None):
        """Clear specific or all cache"""
        if key:
            self._cache.pop(key, None)
            self._cache_time.pop(key, None)
            self._cache_ttl.pop(key, None)
        else:
            self._cache.clear()
            self._cache_time.clear()
            self._cache_ttl.clear()
            self._hit_count = 0
            self._miss_count = 0
    
    def get_stats(self):
        """Get cache statistics"""
        total_requests = self._hit_count + self._miss_count
        hit_rate = round((self._hit_count / total_requests * 100) if total_requests > 0 else 0, 2)
        return {
            'total_items': len(self._cache),
            'keys': list(self._cache.keys()),
            'hit_count': self._hit_count,
            'miss_count': self._miss_count,
            'hit_rate': f"{hit_rate}%",
            'size_kb': round(sum(len(str(v)) for v in self._cache.values()) / 1024, 2)
        }

# Global cache instance
_query_cache = MongoDBQueryCache() if CACHE_ENABLED else None

def cache_key(collection, query, projection=None, sort=None, limit=None):
    """Generate a unique cache key for a query"""
    key_parts = [collection, json.dumps(query, sort_keys=True)]
    if projection:
        key_parts.append(json.dumps(projection, sort_keys=True))
    if sort:
        key_parts.append(json.dumps(sort, sort_keys=True))
    if limit:
        key_parts.append(str(limit))
    return hashlib.md5('_'.join(key_parts).encode()).hexdigest()

# ================================================================
# CONNECTION FUNCTIONS
# ================================================================

def get_connection_options():
    """Get MongoDB connection options optimized for performance"""
    options = {
        'maxPoolSize': MAX_POOL_SIZE,
        'minPoolSize': MIN_POOL_SIZE,
        'maxIdleTimeMS': MAX_IDLE_TIME_MS,
        'connectTimeoutMS': CONNECT_TIMEOUT_MS,
        'socketTimeoutMS': SOCKET_TIMEOUT_MS,
        'serverSelectionTimeoutMS': SERVER_SELECTION_TIMEOUT_MS,
        'retryWrites': True,
        'retryReads': True,
    }
    
    # TLS only for Atlas (not for local)
    if not IS_LOCAL:
        options['tls'] = True
        # CRITICAL FIX: Use ONLY ONE of these options, not both!
        if IS_RENDER:
            # Render environment: use tlsInsecure
            options['tlsInsecure'] = True
            print("⚙️ Render environment: Using tlsInsecure=True")
        else:
            # Local development with Atlas: use tlsAllowInvalidCertificates
            options['tlsAllowInvalidCertificates'] = True
    
    return options

def get_client():
    """Get MongoDB client with retry logic and connection pooling"""
    global _client, _connection_status
    
    # If client exists, verify connection
    if _client is not None:
        try:
            _client.admin.command('ping')
            return _client
        except Exception as e:
            print(f"⚠️ Connection lost: {e}")
            _client = None
            _connection_status['connected'] = False
    
    _connection_status['last_attempt'] = datetime.now()
    
    mode = "Local" if IS_LOCAL else "Atlas (Cloud)"
    print(f"🔌 Connecting to MongoDB {mode}...")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            options = get_connection_options()
            client = MongoClient(MONGO_URI, **options)
            
            # Test connection
            client.admin.command('ping')
            
            server_info = client.server_info()
            print(f"✅ MongoDB Connected! (v{server_info.get('version', 'unknown')})")
            print(f"📍 Mode: {mode}")
            
            _connection_status['connected'] = True
            _connection_status['last_success'] = datetime.now()
            _connection_status['error_count'] = 0
            _connection_status['last_error'] = None
            
            _client = client
            return client
            
        except (ConnectionFailure, ServerSelectionTimeoutError, OperationFailure) as e:
            _connection_status['error_count'] += 1
            _connection_status['last_error'] = str(e)
            
            print(f"❌ Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            
            if attempt < MAX_RETRIES:
                print(f"⏱️  Retrying in {RETRY_DELAY_SECONDS}s...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                print(f"❌ All {MAX_RETRIES} attempts failed")
                
        except InvalidURI as e:
            print(f"❌ Invalid MongoDB URI: {e}")
            break
    
    return None

def get_db():
    """Get default database connection (siliguri_electrical)"""
    global _default_db
    
    client = get_client()
    if client is None:
        return None
    
    if _default_db is None:
        _default_db = client[MONGO_DB_NAME]
        print(f"📁 Default Database: {MONGO_DB_NAME}")
        _print_collections(_default_db)
    
    return _default_db

def get_inventory_db():
    """Get inventory database (siliguri_electrical) - CACHED"""
    global _inventory_db
    
    client = get_client()
    if client is None:
        return None
    
    if _inventory_db is None:
        _inventory_db = client[INVENTORY_DB_NAME]
        print(f"📁 Inventory Database: {INVENTORY_DB_NAME}")
        _print_collections(_inventory_db)
    
    return _inventory_db

def get_user_db():
    """Get user database (ims_siliguri) - CACHED"""
    global _user_db
    
    client = get_client()
    if client is None:
        return None
    
    if _user_db is None:
        _user_db = client[USER_DB_NAME]
        print(f"📁 User Database: {USER_DB_NAME}")
        _print_collections(_user_db)
    
    return _user_db

def _print_collections(db):
    """Print collections in a database"""
    try:
        collections = db.list_collection_names()
        print(f"   📋 Collections: {collections}")
    except Exception as e:
        print(f"   ⚠️ Could not list collections: {e}")

def get_connection_status():
    """Get current connection status"""
    return {
        **_connection_status,
        'default_database': MONGO_DB_NAME,
        'inventory_database': INVENTORY_DB_NAME,
        'user_database': USER_DB_NAME,
        'environment': 'render' if IS_RENDER else 'local',
        'mode': 'local' if IS_LOCAL else 'atlas',
        'cache_enabled': CACHE_ENABLED,
        'cache_stats': _query_cache.get_stats() if _query_cache else None
    }

def close_connection():
    """Close MongoDB connection"""
    global _client, _default_db, _inventory_db, _user_db, _connection_status
    
    if _client:
        _client.close()
        _client = None
        _default_db = None
        _inventory_db = None
        _user_db = None
        _connection_status['connected'] = False
        print("🔌 MongoDB connection closed")

# ================================================================
# CACHED QUERY HELPERS
# ================================================================

def cached_find(collection, query=None, projection=None, sort=None, limit=0, ttl=None):
    """
    Cached version of find - reduces database load for repeated queries
    """
    if not CACHE_ENABLED or _query_cache is None:
        # Fallback to direct query
        db = get_db()
        if db is None:
            return []
        cursor = db[collection].find(query or {}, projection or {})
        if sort:
            cursor = cursor.sort(sort)
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)
    
    # Generate cache key
    key = cache_key(collection, query or {}, projection or {}, sort, limit)
    
    # Try cache
    cached = _query_cache.get(key)
    if cached is not None:
        return cached
    
    # Execute query
    db = get_db()
    if db is None:
        return []
    
    cursor = db[collection].find(query or {}, projection or {})
    if sort:
        cursor = cursor.sort(sort)
    if limit > 0:
        cursor = cursor.limit(limit)
    
    result = list(cursor)
    
    # Cache result
    _query_cache.set(key, result, ttl)
    return result

def cached_find_one(collection, query=None, projection=None, ttl=None):
    """
    Cached version of find_one - reduces database load for repeated queries
    """
    if not CACHE_ENABLED or _query_cache is None:
        db = get_db()
        if db is None:
            return None
        return db[collection].find_one(query or {}, projection or {})
    
    key = cache_key(f"{collection}_one", query or {}, projection or {})
    cached = _query_cache.get(key)
    if cached is not None:
        return cached
    
    db = get_db()
    if db is None:
        return None
    
    result = db[collection].find_one(query or {}, projection or {})
    _query_cache.set(key, result, ttl)
    return result

def clear_cache():
    """Clear query cache"""
    if _query_cache:
        _query_cache.clear()
        print("🗑️ Query cache cleared")

def get_cache_stats():
    """Get cache statistics"""
    if _query_cache:
        return _query_cache.get_stats()
    return {'enabled': False}

# ================================================================
# INVENTORY COLLECTION HELPERS (uses siliguri_electrical)
# ================================================================

def get_inventory_collection(collection_name):
    """Get a collection from inventory database"""
    db = get_inventory_db()
    if db is not None:
        return db[collection_name]
    return None

def inventory_count(collection_name, query=None):
    """Count documents in inventory collection"""
    coll = get_inventory_collection(collection_name)
    if coll is not None:
        return coll.count_documents(query or {})
    return 0

def inventory_find(collection_name, query=None, projection=None, limit=0):
    """Find documents in inventory collection with caching"""
    return cached_find(collection_name, query, projection, limit=limit, ttl=300)

def inventory_find_one(collection_name, query=None, projection=None):
    """Find one document in inventory collection with caching"""
    return cached_find_one(collection_name, query, projection, ttl=300)

# ================================================================
# USER COLLECTION HELPERS (uses ims_siliguri)
# ================================================================

def get_user_collection(collection_name='users'):
    """Get users collection from user database"""
    db = get_user_db()
    if db is not None:
        return db[collection_name]
    return None

def get_all_users():
    """Get all users from user database with caching"""
    return cached_find('users', {}, {'_id': 0}, ttl=3600)  # Cache for 1 hour

def find_user(username):
    """Find a user by username with caching"""
    return cached_find_one('users', {'username': username.lower()}, {'_id': 0}, ttl=3600)

# ================================================================
# GENERAL HELPERS (uses default database)
# ================================================================

def safe_execute(operation, fallback=None):
    """Execute database operation with error handling"""
    try:
        return operation()
    except Exception as e:
        print(f"⚠️ Database operation failed: {e}")
        return fallback

def get_collection(collection_name):
    """Get a collection from default database"""
    db = get_db()
    if db is not None:
        return db[collection_name]
    return None

def count_documents(collection_name, query=None):
    """Count documents in a collection"""
    coll = get_collection(collection_name)
    if coll is not None:
        return coll.count_documents(query or {})
    return 0

def find_documents(collection_name, query=None, projection=None, limit=0):
    """Find documents in a collection with caching"""
    return cached_find(collection_name, query, projection, limit=limit)

def find_one_document(collection_name, query=None, projection=None):
    """Find one document in a collection with caching"""
    return cached_find_one(collection_name, query, projection)

def insert_document(collection_name, document):
    """Insert a document - clears cache for this collection"""
    db = get_db()
    if db is None:
        return None
    
    # Clear cache for this collection
    if _query_cache:
        _query_cache.clear()
    
    result = db[collection_name].insert_one(document)
    return result.inserted_id

def update_document(collection_name, query, update, upsert=False):
    """Update a document - clears cache for this collection"""
    db = get_db()
    if db is None:
        return 0
    
    # Clear cache for this collection
    if _query_cache:
        _query_cache.clear()
    
    result = db[collection_name].update_one(query, update, upsert=upsert)
    return result.modified_count

def delete_document(collection_name, query):
    """Delete a document - clears cache for this collection"""
    db = get_db()
    if db is None:
        return 0
    
    # Clear cache for this collection
    if _query_cache:
        _query_cache.clear()
    
    result = db[collection_name].delete_one(query)
    return result.deleted_count

# ================================================================
# ADMIN FUNCTIONS
# ================================================================

def create_indexes():
    """Create recommended indexes for better performance"""
    db = get_db()
    if db is None:
        return
    
    print("📊 Creating recommended indexes...")
    
    indexes = [
        # Current Stock indexes
        {'collection': 'current_stock', 'keys': [('material_code', 1)]},
        {'collection': 'current_stock', 'keys': [('plant', 1)]},
        {'collection': 'current_stock', 'keys': [('material_group', 1)]},
        
        # Material in Transit indexes
        {'collection': 'material_in_transit', 'keys': [('document_type', 1)]},
        {'collection': 'material_in_transit', 'keys': [('material_code', 1)]},
        {'collection': 'material_in_transit', 'keys': [('from_plant', 1)]},
        {'collection': 'material_in_transit', 'keys': [('to_plant', 1)]},
        
        # Consumption Summary indexes
        {'collection': 'consumption_summary', 'keys': [('material_code', 1)]},
        {'collection': 'consumption_summary', 'keys': [('period', 1)]},
        {'collection': 'consumption_summary', 'keys': [('plant', 1)]},
        {'collection': 'consumption_summary', 'keys': [('material_group', 1)]},
        
        # Inventory Transactions indexes
        {'collection': 'inventory_transactions', 'keys': [('material_code', 1)]},
        {'collection': 'inventory_transactions', 'keys': [('period', 1)]},
        {'collection': 'inventory_transactions', 'keys': [('plant', 1)]},
    ]
    
    for idx in indexes:
        try:
            collection = db[idx['collection']]
            existing = collection.index_information()
            key_str = str(idx['keys'])
            if key_str not in existing:
                collection.create_index(idx['keys'])
                print(f"   ✅ Created index on {idx['collection']}: {idx['keys']}")
        except Exception as e:
            print(f"   ⚠️ Could not create index: {e}")

# ================================================================
# INITIALIZATION
# ================================================================

print("=" * 60)
print("🔌 MongoDB Connection Initializer")
print("=" * 60)
print(f"🌍 Environment: {'Render' if IS_RENDER else 'Local'}")
print(f"📍 Mode: {'Local' if IS_LOCAL else 'Atlas (Cloud)'}")
print(f"📁 Default DB: {MONGO_DB_NAME}")
print(f"📁 Inventory DB: {INVENTORY_DB_NAME}")
print(f"📁 User DB: {USER_DB_NAME}")
print(f"💾 Cache Enabled: {CACHE_ENABLED}")
print("=" * 60)

# Test connections on import
client = get_client()
if client is not None:
    # Test inventory database
    inv_db = get_inventory_db()
    if inv_db is not None:
        try:
            inv_collections = inv_db.list_collection_names()
            print(f"✅ Inventory DB ({INVENTORY_DB_NAME}): {len(inv_collections)} collections")
            
            # Check critical collections
            critical = ['current_stock', 'material_in_transit', 'consumption_summary', 'inventory_transactions', 'storage_locations', 'material_master']
            for coll in critical:
                if coll in inv_collections:
                    count = inv_db[coll].count_documents({})
                    print(f"   ✅ {coll}: {count:,} records")
                else:
                    print(f"   ❌ {coll}: NOT FOUND")
        except Exception as e:
            print(f"⚠️ Could not check inventory collections: {e}")
    
    # Test user database
    user_db = get_user_db()
    if user_db is not None:
        try:
            user_collections = user_db.list_collection_names()
            print(f"✅ User DB ({USER_DB_NAME}): {len(user_collections)} collections")
            
            if 'users' in user_collections:
                count = user_db.users.count_documents({})
                print(f"   ✅ users: {count} records")
            else:
                print(f"   ❌ users: NOT FOUND")
        except Exception as e:
            print(f"⚠️ Could not check user collections: {e}")
    
    # Create indexes (optional)
    if not IS_LOCAL:  # Only create indexes on production
        try:
            create_indexes()
        except Exception as e:
            print(f"⚠️ Could not create indexes: {e}")
else:
    print("⚠️ MongoDB not connected - app will use fallback data")

print("=" * 60)