"""
MongoDB Database Helper for IMS Siliguri
Enhanced version with SSL support, connection pooling, and better error handling
Last Updated: March 4, 2026
FIXED: Added SSL options for Render deployment
FIXED: Connection pooling for better performance
FIXED: Environment-specific configurations
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
from datetime import datetime
import os
import time
import socket
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==================== CONFIGURATION ====================

# MongoDB Connection
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'siliguri_electrical')

# Connection Pool Settings
MAX_POOL_SIZE = int(os.getenv('MONGO_MAX_POOL_SIZE', '50'))
MIN_POOL_SIZE = int(os.getenv('MONGO_MIN_POOL_SIZE', '10'))
MAX_IDLE_TIME_MS = int(os.getenv('MONGO_MAX_IDLE_TIME_MS', '10000'))
CONNECT_TIMEOUT_MS = int(os.getenv('MONGO_CONNECT_TIMEOUT_MS', '20000'))
SOCKET_TIMEOUT_MS = int(os.getenv('MONGO_SOCKET_TIMEOUT_MS', '20000'))
SERVER_SELECTION_TIMEOUT_MS = int(os.getenv('MONGO_SERVER_SELECTION_TIMEOUT_MS', '30000'))

# SSL/TLS Settings
SSL_ENABLED = os.getenv('MONGO_SSL_ENABLED', 'true').lower() == 'true'
SSL_ALLOW_INVALID_CERT = os.getenv('MONGO_SSL_ALLOW_INVALID_CERT', 'false').lower() == 'true'

# Retry Settings
MAX_RETRIES = int(os.getenv('MONGO_MAX_RETRIES', '3'))
RETRY_DELAY_SECONDS = int(os.getenv('MONGO_RETRY_DELAY_SECONDS', '2'))

# Global database connection
mongo_client = None
db = None
connection_status = {
    'connected': False,
    'last_attempt': None,
    'last_success': None,
    'error_count': 0,
    'last_error': None
}

# ==================== CONNECTION FUNCTIONS ====================

def get_connection_options():
    """Get MongoDB connection options based on environment"""
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
    
    # Add SSL options if enabled
    if SSL_ENABLED:
        options['tls'] = True
        options['tlsAllowInvalidCertificates'] = SSL_ALLOW_INVALID_CERT
        
        # For Render deployment, we need to be more flexible with SSL
        if os.getenv('RENDER', 'false').lower() == 'true':
            options['tlsInsecure'] = True
            print("⚙️ Render environment detected: Using relaxed SSL settings")
    
    return options

def test_network_connectivity():
    """Test basic network connectivity to MongoDB host"""
    try:
        # Extract host from URI
        if 'mongodb.net' in MONGO_URI:
            # For Atlas, extract hostname
            import re
            match = re.search(r'@([^/]+)', MONGO_URI)
            if match:
                host = match.group(1).split('?')[0]
                # Test DNS resolution
                socket.gethostbyname(host)
                print(f"✅ DNS resolution successful for {host}")
                
                # Test basic connectivity (not full SSL handshake)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                ip = socket.gethostbyname(host)
                result = sock.connect_ex((ip, 27017))
                sock.close()
                
                if result == 0:
                    print(f"✅ Basic network connectivity to {host}:27017")
                else:
                    print(f"⚠️ Cannot connect to {host}:27017 - check firewall/network access")
            return True
    except Exception as e:
        print(f"⚠️ Network test failed: {e}")
        return False

def create_mongo_client():
    """Create MongoDB client with proper options and retry logic"""
    global connection_status
    
    connection_status['last_attempt'] = datetime.now()
    
    # Test basic network first
    test_network_connectivity()
    
    # Get connection options
    options = get_connection_options()
    
    print(f"🔌 Attempting to connect to MongoDB...")
    print(f"📊 Connection pool: min={MIN_POOL_SIZE}, max={MAX_POOL_SIZE}")
    print(f"⏱️  Timeouts: connect={CONNECT_TIMEOUT_MS}ms, socket={SOCKET_TIMEOUT_MS}ms")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"🔄 Connection attempt {attempt}/{MAX_RETRIES}...")
            
            # Create client
            client = MongoClient(MONGO_URI, **options)
            
            # Test connection with ping
            client.admin.command('ping')
            
            # Get server info
            server_info = client.server_info()
            mongodb_version = server_info.get('version', 'unknown')
            
            print(f"✅ MongoDB Connected Successfully!")
            print(f"📦 Server version: {mongodb_version}")
            print(f"📦 Database: {MONGO_DB}")
            
            connection_status['connected'] = True
            connection_status['last_success'] = datetime.now()
            connection_status['error_count'] = 0
            connection_status['last_error'] = None
            
            return client
            
        except (ConnectionFailure, ServerSelectionTimeoutError, OperationFailure) as e:
            error_msg = str(e)
            connection_status['error_count'] += 1
            connection_status['last_error'] = error_msg
            
            print(f"❌ Attempt {attempt} failed: {error_msg}")
            
            if attempt < MAX_RETRIES:
                print(f"⏱️  Waiting {RETRY_DELAY_SECONDS} seconds before retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                print(f"❌ All {MAX_RETRIES} connection attempts failed")
                
    return None

def get_db():
    """Get database connection with automatic reconnection"""
    global mongo_client, db, connection_status
    
    # Check if we need to reconnect
    if mongo_client is None:
        mongo_client = create_mongo_client()
        if mongo_client:
            db = mongo_client[MONGO_DB]
    else:
        # Verify connection is still alive
        try:
            mongo_client.admin.command('ping')
        except Exception as e:
            print(f"⚠️ Connection lost: {e}. Attempting to reconnect...")
            try:
                mongo_client.close()
            except:
                pass
            mongo_client = create_mongo_client()
            if mongo_client:
                db = mongo_client[MONGO_DB]
    
    return db

def get_connection_status():
    """Get current connection status"""
    global connection_status
    return {
        **connection_status,
        'uri_masked': MONGO_URI.replace(MONGO_URI.split('@')[0].split(':')[1] if '@' in MONGO_URI else '', '****'),
        'database': MONGO_DB,
        'pool_size': MAX_POOL_SIZE
    }

def close_connection():
    """Close MongoDB connection"""
    global mongo_client, db, connection_status
    if mongo_client:
        mongo_client.close()
        mongo_client = None
        db = None
        connection_status['connected'] = False
        print("🔌 MongoDB connection closed")

# Initialize connection
db = get_db()

# ==================== EXISTING CLASSES (Enhanced with error handling) ====================

class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass

class ProjectDB:
    """Helper class for project operations"""
    
    @staticmethod
    def safe_execute(operation, fallback=None):
        """Execute database operation with error handling"""
        try:
            return operation()
        except Exception as e:
            print(f"⚠️ Database operation failed: {e}")
            # Try to reconnect
            global mongo_client, db
            if mongo_client:
                try:
                    mongo_client.admin.command('ping')
                except:
                    print("🔄 Attempting to reconnect...")
                    close_connection()
                    get_db()
            return fallback
    
    @staticmethod
    def get_user_info(user_id):
        """Get user's role and division from profiles"""
        def _operation():
            db = get_db()
            if db is not None:
                user = db.users.find_one({"_id": user_id})
                if user:
                    return {
                        'role': user.get('role', 'user'),
                        'region': user.get('region'),
                        'division': user.get('division'),
                        'section': user.get('section'),
                        'email': user.get('email')
                    }
            return None
        
        return ProjectDB.safe_execute(_operation, None)
    
    @staticmethod
    def create_project(user_id, project_data):
        """Create a new project"""
        def _operation():
            db = get_db()
            if db is None:
                print("Database not connected")
                return None
                
            user_info = ProjectDB.get_user_info(user_id)
            
            # Prepare project document
            project = {
                "_id": f"proj_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "created_by": user_id,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "project_name": project_data.get('project_name'),
                "project_type": project_data.get('project_type'),
                "priority": project_data.get('priority', 'Phase-I'),
                "region": project_data.get('region', user_info.get('region') if user_info else None),
                "division": project_data.get('division', user_info.get('division') if user_info else None),
                "status": project_data.get('status', 'active'),
                "progress_percentage": project_data.get('progress_percentage', 0),
                "project_id": project_data.get('project_id'),
                "data": {}
            }
            
            # Put ALL other fields into the data object
            for key, value in project_data.items():
                if key not in project and key != 'created_by':
                    project['data'][key] = value
            
            # Insert into MongoDB
            result = db.projects.insert_one(project)
            if result.inserted_id:
                project['_id'] = str(result.inserted_id)
                return project
            return None
        
        return ProjectDB.safe_execute(_operation, None)
    
    @staticmethod
    def get_projects(user_id, filters=None):
        """Get projects based on user's role and division"""
        def _operation():
            db = get_db()
            if db is None:
                print("Database not connected")
                return []
                
            user_info = ProjectDB.get_user_info(user_id)
            if not user_info:
                return []
            
            # Build query
            query = {}
            
            # If not admin/authority, filter by division/region
            if user_info['role'] not in ['admin', 'authority']:
                if user_info.get('division'):
                    query['division'] = user_info['division']
                elif user_info.get('region'):
                    query['region'] = user_info['region']
            
            # Apply additional filters
            if filters:
                for key, value in filters.items():
                    if key != 'data' and value:
                        query[key] = value
            
            projects = list(db.projects.find(query))
            
            # Convert ObjectId to string
            for p in projects:
                p['_id'] = str(p['_id'])
            
            return projects
        
        return ProjectDB.safe_execute(_operation, [])
    
    @staticmethod
    def get_project_by_id(user_id, project_id):
        """Get a single project by ID"""
        def _operation():
            db = get_db()
            if db is None:
                print("Database not connected")
                return None
                
            project = db.projects.find_one({"_id": project_id})
            if project:
                project['_id'] = str(project['_id'])
                return project
            return None
        
        return ProjectDB.safe_execute(_operation, None)
    
    @staticmethod
    def update_project(user_id, project_id, updates):
        """Update a project"""
        def _operation():
            db = get_db()
            if db is None:
                print("Database not connected")
                return None
                
            updates['updated_at'] = datetime.now()
            result = db.projects.update_one(
                {"_id": project_id},
                {"$set": updates}
            )
            if result.modified_count > 0:
                return ProjectDB.get_project_by_id(user_id, project_id)
            return None
        
        return ProjectDB.safe_execute(_operation, None)
    
    @staticmethod
    def delete_project(user_id, project_id):
        """Delete a project"""
        def _operation():
            db = get_db()
            if db is None:
                print("Database not connected")
                return False
                
            result = db.projects.delete_one({"_id": project_id})
            return result.deleted_count > 0
        
        return ProjectDB.safe_execute(_operation, False)


class ReferenceDB:
    """Helper for reference data"""
    
    @staticmethod
    def safe_execute(operation, fallback=None):
        try:
            return operation()
        except Exception as e:
            print(f"⚠️ Reference operation failed: {e}")
            return fallback
    
    @staticmethod
    def get_divisions():
        def _operation():
            db = get_db()
            if db is not None:
                divisions = list(db.divisions.find({}))
                return [d.get('name') for d in divisions]
            return ['Siliguri Town', 'Kurseong', 'Darjeeling', 'Jalpaiguri', 'Coochbehar']
        
        return ReferenceDB.safe_execute(_operation, 
            ['Siliguri Town', 'Kurseong', 'Darjeeling', 'Jalpaiguri', 'Coochbehar'])
    
    @staticmethod
    def get_regions():
        def _operation():
            db = get_db()
            if db is not None:
                regions = list(db.regions.find({}))
                return [r.get('name') for r in regions]
            return ['Darjeeling', 'Jalpaiguri', 'Coochbehar', 'Alipurduar']
        
        return ReferenceDB.safe_execute(_operation,
            ['Darjeeling', 'Jalpaiguri', 'Coochbehar', 'Alipurduar'])
    
    @staticmethod
    def get_project_types():
        return ['New Substation', 'PTR Augmentation', 'New 33KV Line', 
                '33KV Conductor Augmentation', 'New 11KV Line', 
                '11KV Conductor Augmentation', 'HVDS']
    
    @staticmethod
    def get_priorities():
        return ['Phase-I', 'Phase-II', 'Phase-III', 'High', 'Medium', 'Low']
    
    @staticmethod
    def get_statuses():
        return ['active', 'completed', 'on-hold', 'cancelled']


class FilterDB:
    """Helper for filtered queries"""
    
    @staticmethod
    def safe_execute(operation, fallback=None):
        try:
            return operation()
        except Exception as e:
            print(f"⚠️ Filter operation failed: {e}")
            return fallback
    
    @staticmethod
    def get_all_regions():
        """Get all regions with stats"""
        def _operation():
            db = get_db()
            if db is None:
                print("Database not connected, returning sample data")
                return [
                    {"id": "reg_darjeeling", "name": "Darjeeling", "substations": 10, "divisions": 5},
                    {"id": "reg_jalpaiguri", "name": "Jalpaiguri", "substations": 6, "divisions": 2},
                    {"id": "reg_coochbehar", "name": "Coochbehar", "substations": 5, "divisions": 3},
                    {"id": "reg_alipurduar", "name": "Alipurduar", "substations": 3, "divisions": 1}
                ]
                
            regions = list(db.regions.find({}))
            result = []
            for r in regions:
                # Get divisions in this region
                divisions = list(db.divisions.find({"region_id": r["_id"]}))
                div_ids = [d["_id"] for d in divisions]
                
                # Count substations
                substations = db.substations.count_documents({"division_id": {"$in": div_ids}})
                
                result.append({
                    "id": r["_id"],
                    "name": r["name"],
                    "substations": substations,
                    "divisions": len(divisions)
                })
            return result
        
        return FilterDB.safe_execute(_operation,
            [
                {"id": "reg_darjeeling", "name": "Darjeeling", "substations": 10, "divisions": 5},
                {"id": "reg_jalpaiguri", "name": "Jalpaiguri", "substations": 6, "divisions": 2},
                {"id": "reg_coochbehar", "name": "Coochbehar", "substations": 5, "divisions": 3},
                {"id": "reg_alipurduar", "name": "Alipurduar", "substations": 3, "divisions": 1}
            ])
    
    @staticmethod
    def get_region_by_id(region_id):
        """Get region by ID"""
        def _operation():
            db = get_db()
            if db is not None:
                region = db.regions.find_one({"_id": region_id})
                if region:
                    return region
            return {"_id": region_id, "name": region_id.capitalize()}
        
        return FilterDB.safe_execute(_operation, {"_id": region_id, "name": region_id.capitalize()})
    
    @staticmethod
    def get_divisions_by_region(region_id):
        """Get divisions for a region"""
        def _operation():
            db = get_db()
            if db is None:
                # Return sample data
                if region_id == 'reg_darjeeling':
                    return [
                        {"id": "div_siliguri", "name": "Siliguri Town", "substation_count": 4},
                        {"id": "div_kurseong", "name": "Kurseong", "substation_count": 3},
                        {"id": "div_darjeeling", "name": "Darjeeling", "substation_count": 2},
                        {"id": "div_suburban", "name": "Sub-Urban", "substation_count": 1}
                    ]
                return []
                
            divisions = list(db.divisions.find({"region_id": region_id}))
            result = []
            for d in divisions:
                # Count substations in this division
                substations = db.substations.count_documents({"division_id": d["_id"]})
                result.append({
                    "id": d["_id"],
                    "name": d["name"],
                    "substation_count": substations
                })
            return result
        
        return FilterDB.safe_execute(_operation, [])
    
    @staticmethod
    def get_division_by_id(division_id):
        """Get division by ID"""
        def _operation():
            db = get_db()
            if db is not None:
                division = db.divisions.find_one({"_id": division_id})
                if division:
                    return division
            return {"_id": division_id, "name": division_id.capitalize(), "region_id": "reg_darjeeling"}
        
        return FilterDB.safe_execute(_operation, 
            {"_id": division_id, "name": division_id.capitalize(), "region_id": "reg_darjeeling"})
    
    @staticmethod
    def get_substations_by_division(division_id):
        """Get substations for a division"""
        def _operation():
            db = get_db()
            if db is None:
                return [
                    {"id": "ss_city_center", "name": "City Center", "location": "City Center", "capacity": "40 MVA", "status": "Active"},
                    {"id": "ss_industrial", "name": "Industrial Area", "location": "Industrial Zone", "capacity": "30 MVA", "status": "Active"}
                ]
                
            substations = list(db.substations.find({"division_id": division_id}))
            result = []
            for s in substations:
                result.append({
                    "id": s["_id"],
                    "name": s["name"],
                    "location": s.get("location", ""),
                    "capacity": s.get("capacity", ""),
                    "status": s.get("status", "Active")
                })
            return result
        
        return FilterDB.safe_execute(_operation, [])
    
    @staticmethod
    def get_substation_by_id(substation_id):
        """Get substation by ID"""
        def _operation():
            db = get_db()
            if db is not None:
                substation = db.substations.find_one({"_id": substation_id})
                if substation:
                    return substation
            return {
                "_id": substation_id,
                "name": "City Center",
                "division_id": "div_siliguri",
                "location": "City Center",
                "capacity": "40 MVA",
                "status": "Active"
            }
        
        return FilterDB.safe_execute(_operation, {
            "_id": substation_id,
            "name": "City Center",
            "division_id": "div_siliguri",
            "location": "City Center",
            "capacity": "40 MVA",
            "status": "Active"
        })
    
    @staticmethod
    def get_ptrs_by_substation(substation_id):
        """Get PTRs for a substation"""
        def _operation():
            db = get_db()
            if db is None:
                return [
                    {"_id": "ptr_001", "name": "PTR-1", "capacity": "20 MVA", "status": "Active"},
                    {"_id": "ptr_002", "name": "PTR-2", "capacity": "20 MVA", "status": "Active"}
                ]
            return list(db.ptr_units.find({"substation_id": substation_id}))
        
        return FilterDB.safe_execute(_operation, [])
    
    @staticmethod
    def get_33kv_lines_by_substation(substation_id):
        """Get 33KV lines from a substation"""
        def _operation():
            db = get_db()
            if db is None:
                return [
                    {"_id": "line_001", "name": "Siliguri - Bagdogra", "length_km": 22.5, "status": "Active"},
                    {"_id": "line_002", "name": "Siliguri - Kurseong", "length_km": 24.0, "status": "Active"}
                ]
            return list(db.lines_33kv.find({"from_substation_id": substation_id}))
        
        return FilterDB.safe_execute(_operation, [])
    
    @staticmethod
    def get_11kv_feeders_by_substation(substation_id):
        """Get 11KV feeders from a substation"""
        def _operation():
            db = get_db()
            if db is None:
                return [
                    {"_id": "fdr_001", "name": "Feeder A", "length_km": 8.5, "dtr_count": 24, "status": "Active"},
                    {"_id": "fdr_002", "name": "Feeder B", "length_km": 12.0, "dtr_count": 32, "status": "Active"}
                ]
            return list(db.feeders_11kv.find({"substation_id": substation_id}))
        
        return FilterDB.safe_execute(_operation, [])
    
    @staticmethod
    def get_substation_details(substation_id):
        """Get complete substation details with counts"""
        try:
            substation = FilterDB.get_substation_by_id(substation_id)
            ptrs = FilterDB.get_ptrs_by_substation(substation_id)
            lines = FilterDB.get_33kv_lines_by_substation(substation_id)
            feeders = FilterDB.get_11kv_feeders_by_substation(substation_id)
            
            return {
                "id": substation["_id"],
                "name": substation["name"],
                "location": substation.get("location", ""),
                "capacity": substation.get("capacity", ""),
                "current_load": "32 MVA",
                "ptr_count": len(ptrs),
                "lines_33kv": len(lines),
                "feeders_11kv": len(feeders),
                "dtr_count": sum(f.get("dtr_count", 0) for f in feeders),
                "status": substation.get("status", "Active"),
                "commission_date": substation.get("commission_date", "2018-01-01")
            }
        except Exception as e:
            print(f"Error getting substation details: {e}")
            return {
                "id": substation_id,
                "name": "City Center",
                "location": "City Center",
                "capacity": "40 MVA",
                "current_load": "32 MVA",
                "ptr_count": 2,
                "lines_33kv": 4,
                "feeders_11kv": 4,
                "dtr_count": 89,
                "status": "Active",
                "commission_date": "2018-05-15"
            }
    
    @staticmethod
    def get_projects_by_type_and_location(project_type, substation_id=None):
        """Get projects by type and location"""
        def _operation():
            db = get_db()
            if db is None:
                return []
                
            query = {"category": project_type}
            if substation_id:
                query["substation_id"] = substation_id
            return list(db.projects.find(query))
        
        return FilterDB.safe_execute(_operation, [])


def init_master_data():
    """Initialize master data if collections are empty"""
    try:
        db = get_db()
        if db is None:
            print("⚠️ Database not connected, skipping master data initialization")
            return
        
        # Check if regions exist
        if db.regions.count_documents({}) == 0:
            print("📦 Initializing regions...")
            db.regions.insert_many([
                {"_id": "reg_darjeeling", "name": "Darjeeling", "code": "DAR"},
                {"_id": "reg_jalpaiguri", "name": "Jalpaiguri", "code": "JAL"},
                {"_id": "reg_coochbehar", "name": "Coochbehar", "code": "COB"},
                {"_id": "reg_alipurduar", "name": "Alipurduar", "code": "ALI"}
            ])
        
        # Check if divisions exist
        if db.divisions.count_documents({}) == 0:
            print("📦 Initializing divisions...")
            db.divisions.insert_many([
                {"_id": "div_siliguri", "name": "Siliguri Town", "region_id": "reg_darjeeling"},
                {"_id": "div_kurseong", "name": "Kurseong", "region_id": "reg_darjeeling"},
                {"_id": "div_darjeeling", "name": "Darjeeling", "region_id": "reg_darjeeling"},
                {"_id": "div_suburban", "name": "Sub-Urban", "region_id": "reg_darjeeling"},
                {"_id": "div_kalimpong", "name": "Kalimpong", "region_id": "reg_darjeeling"},
                {"_id": "div_jalpaiguri", "name": "Jalpaiguri", "region_id": "reg_jalpaiguri"},
                {"_id": "div_mal", "name": "Mal", "region_id": "reg_jalpaiguri"},
                {"_id": "div_coochbehar", "name": "Coochbehar", "region_id": "reg_coochbehar"},
                {"_id": "div_mathabhanga", "name": "Mathabhanga", "region_id": "reg_coochbehar"},
                {"_id": "div_dinhata", "name": "Dinhata", "region_id": "reg_coochbehar"},
                {"_id": "div_alipurduar", "name": "Alipurduar", "region_id": "reg_alipurduar"}
            ])
        
        print("✅ Master data initialized successfully")
    except Exception as e:
        print(f"❌ Error initializing master data: {e}")

# ==================== CONNECTION TEST FUNCTION ====================

def test_connection():
    """Test MongoDB connection and return status"""
    db = get_db()
    if db is not None:
        try:
            # Try to list collections
            collections = db.list_collection_names()
            return {
                'status': 'connected',
                'database': MONGO_DB,
                'collections': len(collections),
                'connection_status': get_connection_status()
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'connection_status': get_connection_status()
            }
    else:
        return {
            'status': 'disconnected',
            'connection_status': get_connection_status()
        }

# Initialize connection on import
if __name__ != '__main__':
    # When imported as module, test connection
    test_result = test_connection()
    if test_result['status'] == 'connected':
        print(f"✅ MongoDB ready: {test_result['collections']} collections available")
    else:
        print(f"⚠️ MongoDB status: {test_result['status']}") 
