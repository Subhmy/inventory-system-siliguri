"""
MongoDB Database Helper for IMS Siliguri
Enhanced version with all filter functions
FIXED: Replaced all 'if db:' with 'if db is not None'
"""

from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Connection
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'siliguri_electrical')

# Global database connection
mongo_client = None
db = None

def get_db():
    """Get database connection"""
    global mongo_client, db
    try:
        if mongo_client is None:
            mongo_client = MongoClient(MONGO_URI)
            db = mongo_client[MONGO_DB]
            print("✅ MongoDB Connected Successfully")
        return db
    except Exception as e:
        print(f"❌ MongoDB Connection Error: {e}")
        return None

# Initialize connection
db = get_db()

class ProjectDB:
    """Helper class for project operations"""
    
    @staticmethod
    def get_user_info(user_id):
        """Get user's role and division from profiles"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error getting user info: {e}")
            return None
    
    @staticmethod
    def create_project(user_id, project_data):
        """Create a new project"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error creating project: {e}")
            return None
    
    @staticmethod
    def get_projects(user_id, filters=None):
        """Get projects based on user's role and division"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error getting projects: {e}")
            return []
    
    @staticmethod
    def get_project_by_id(user_id, project_id):
        """Get a single project by ID"""
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is None:
                print("Database not connected")
                return None
                
            project = db.projects.find_one({"_id": project_id})
            if project:
                project['_id'] = str(project['_id'])
                return project
            return None
        except Exception as e:
            print(f"Error getting project: {e}")
            return None
    
    @staticmethod
    def update_project(user_id, project_id, updates):
        """Update a project"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error updating project: {e}")
            return None
    
    @staticmethod
    def delete_project(user_id, project_id):
        """Delete a project"""
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is None:
                print("Database not connected")
                return False
                
            result = db.projects.delete_one({"_id": project_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting project: {e}")
            return False


class ReferenceDB:
    """Helper for reference data"""
    
    @staticmethod
    def get_divisions():
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is not None:
                divisions = list(db.divisions.find({}))
                return [d.get('name') for d in divisions]
            return ['Siliguri Town', 'Kurseong', 'Darjeeling', 'Jalpaiguri', 'Coochbehar']
        except Exception as e:
            print(f"Error getting divisions: {e}")
            return ['Siliguri Town', 'Kurseong', 'Darjeeling', 'Jalpaiguri', 'Coochbehar']
    
    @staticmethod
    def get_regions():
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is not None:
                regions = list(db.regions.find({}))
                return [r.get('name') for r in regions]
            return ['Darjeeling', 'Jalpaiguri', 'Coochbehar', 'Alipurduar']
        except Exception as e:
            print(f"Error getting regions: {e}")
            return ['Darjeeling', 'Jalpaiguri', 'Coochbehar', 'Alipurduar']
    
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
    def get_all_regions():
        """Get all regions with stats"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error getting regions: {e}")
            return [
                {"id": "reg_darjeeling", "name": "Darjeeling", "substations": 10, "divisions": 5},
                {"id": "reg_jalpaiguri", "name": "Jalpaiguri", "substations": 6, "divisions": 2},
                {"id": "reg_coochbehar", "name": "Coochbehar", "substations": 5, "divisions": 3},
                {"id": "reg_alipurduar", "name": "Alipurduar", "substations": 3, "divisions": 1}
            ]
    
    @staticmethod
    def get_region_by_id(region_id):
        """Get region by ID"""
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is not None:
                region = db.regions.find_one({"_id": region_id})
                if region:
                    return region
            return {"_id": region_id, "name": region_id.capitalize()}
        except Exception as e:
            print(f"Error getting region: {e}")
            return {"_id": region_id, "name": region_id.capitalize()}
    
    @staticmethod
    def get_divisions_by_region(region_id):
        """Get divisions for a region"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error getting divisions: {e}")
            # Return sample data
            if region_id == 'reg_darjeeling':
                return [
                    {"id": "div_siliguri", "name": "Siliguri Town", "substation_count": 4},
                    {"id": "div_kurseong", "name": "Kurseong", "substation_count": 3},
                    {"id": "div_darjeeling", "name": "Darjeeling", "substation_count": 2},
                    {"id": "div_suburban", "name": "Sub-Urban", "substation_count": 1}
                ]
            return []
    
    @staticmethod
    def get_division_by_id(division_id):
        """Get division by ID"""
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is not None:
                division = db.divisions.find_one({"_id": division_id})
                if division:
                    return division
            return {"_id": division_id, "name": division_id.capitalize(), "region_id": "reg_darjeeling"}
        except Exception as e:
            print(f"Error getting division: {e}")
            return {"_id": division_id, "name": division_id.capitalize(), "region_id": "reg_darjeeling"}
    
    @staticmethod
    def get_substations_by_division(division_id):
        """Get substations for a division"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error getting substations: {e}")
            return [
                {"id": "ss_city_center", "name": "City Center", "location": "City Center", "capacity": "40 MVA", "status": "Active"},
                {"id": "ss_industrial", "name": "Industrial Area", "location": "Industrial Zone", "capacity": "30 MVA", "status": "Active"}
            ]
    
    @staticmethod
    def get_substation_by_id(substation_id):
        """Get substation by ID"""
        try:
            db = get_db()
            # FIXED: Compare with None
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
        except Exception as e:
            print(f"Error getting substation: {e}")
            return {
                "_id": substation_id,
                "name": "City Center",
                "division_id": "div_siliguri",
                "location": "City Center",
                "capacity": "40 MVA",
                "status": "Active"
            }
    
    @staticmethod
    def get_ptrs_by_substation(substation_id):
        """Get PTRs for a substation"""
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is None:
                return [
                    {"_id": "ptr_001", "name": "PTR-1", "capacity": "20 MVA", "status": "Active"},
                    {"_id": "ptr_002", "name": "PTR-2", "capacity": "20 MVA", "status": "Active"}
                ]
            return list(db.ptr_units.find({"substation_id": substation_id}))
        except Exception as e:
            print(f"Error getting PTRs: {e}")
            return [
                {"_id": "ptr_001", "name": "PTR-1", "capacity": "20 MVA", "status": "Active"},
                {"_id": "ptr_002", "name": "PTR-2", "capacity": "20 MVA", "status": "Active"}
            ]
    
    @staticmethod
    def get_33kv_lines_by_substation(substation_id):
        """Get 33KV lines from a substation"""
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is None:
                return [
                    {"_id": "line_001", "name": "Siliguri - Bagdogra", "length_km": 22.5, "status": "Active"},
                    {"_id": "line_002", "name": "Siliguri - Kurseong", "length_km": 24.0, "status": "Active"}
                ]
            return list(db.lines_33kv.find({"from_substation_id": substation_id}))
        except Exception as e:
            print(f"Error getting 33KV lines: {e}")
            return [
                {"_id": "line_001", "name": "Siliguri - Bagdogra", "length_km": 22.5, "status": "Active"},
                {"_id": "line_002", "name": "Siliguri - Kurseong", "length_km": 24.0, "status": "Active"}
            ]
    
    @staticmethod
    def get_11kv_feeders_by_substation(substation_id):
        """Get 11KV feeders from a substation"""
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is None:
                return [
                    {"_id": "fdr_001", "name": "Feeder A", "length_km": 8.5, "dtr_count": 24, "status": "Active"},
                    {"_id": "fdr_002", "name": "Feeder B", "length_km": 12.0, "dtr_count": 32, "status": "Active"}
                ]
            return list(db.feeders_11kv.find({"substation_id": substation_id}))
        except Exception as e:
            print(f"Error getting 11KV feeders: {e}")
            return [
                {"_id": "fdr_001", "name": "Feeder A", "length_km": 8.5, "dtr_count": 24, "status": "Active"},
                {"_id": "fdr_002", "name": "Feeder B", "length_km": 12.0, "dtr_count": 32, "status": "Active"}
            ]
    
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
        try:
            db = get_db()
            # FIXED: Compare with None
            if db is None:
                return []
                
            query = {"category": project_type}
            if substation_id:
                query["substation_id"] = substation_id
            return list(db.projects.find(query))
        except Exception as e:
            print(f"Error getting projects: {e}")
            return []


def init_master_data():
    """Initialize master data if collections are empty"""
    try:
        db = get_db()
        # FIXED: Compare with None instead of 'if db:'
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