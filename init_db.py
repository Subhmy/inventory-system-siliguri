"""
Initialize MongoDB collections and indexes
Run this once to set up your database
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def init_database():
    """Initialize MongoDB collections and indexes"""
    
    print("="*50)
    print("MONGODB INITIALIZATION")
    print("="*50)
    
    # Connect to MongoDB
    mongo_uri = os.getenv('MONGO_URI')
    if not mongo_uri:
        print("❌ MONGO_URI not found in .env file")
        return False
    
    try:
        client = MongoClient(mongo_uri)
        # Test connection
        client.admin.command('ping')
        print("✅ Connected to MongoDB Atlas")
        
        # Get database name from URI or use default
        db_name = mongo_uri.split('/')[-1].split('?')[0]
        if not db_name:
            db_name = 'ims_database'
        
        db = client[db_name]
        print(f"📁 Using database: {db_name}")
        
    except ConnectionFailure:
        print("❌ Failed to connect to MongoDB. Check your connection string.")
        return False
    
    # ==================== USERS COLLECTION ====================
    print("\n📁 Setting up users collection...")
    
    # Create unique index on email
    db.users.create_index('email', unique=True)
    print("✅ Created unique index on email")
    
    # Create index on role for faster queries
    db.users.create_index('role')
    print("✅ Created index on role")
    
    # ==================== PROJECTS COLLECTION ====================
    print("\n📁 Setting up projects collection...")
    
    db.projects.create_index('created_by')
    db.projects.create_index('project_type')
    db.projects.create_index('division')
    db.projects.create_index('status')
    db.projects.create_index([('created_at', -1)])  # Descending for latest first
    print("✅ Created indexes on projects collection")
    
    # ==================== ASSETS COLLECTION ====================
    print("\n📁 Setting up assets collection...")
    
    # For your infrastructure data (substations, lines, feeders)
    db.assets.create_index('asset_id', unique=True)
    db.assets.create_index('asset_type')
    db.assets.create_index('region')
    db.assets.create_index('division')
    db.assets.create_index('parent_id')
    print("✅ Created indexes on assets collection")
    
    # ==================== MONTHLY READINGS COLLECTION ====================
    print("\n📁 Setting up monthly_readings collection...")
    
    db.monthly_readings.create_index([('asset_id', 1), ('month', 1)], unique=True)
    db.monthly_readings.create_index('month')
    db.monthly_readings.create_index('asset_id')
    print("✅ Created indexes on monthly_readings collection")
    
    # ==================== CREATE TEST ADMIN USER ====================
    print("\n👤 Creating test admin user...")
    
    from werkzeug.security import generate_password_hash
    
    test_user = db.users.find_one({'email': 'admin@siliguri.com'})
    if not test_user:
        admin_user = {
            'email': 'admin@siliguri.com',
            'password_hash': generate_password_hash('Admin123!'),
            'full_name': 'System Administrator',
            'role': 'authority',
            'region': 'Darjeeling',
            'division': 'Siliguri Town',
            'created_at': datetime.utcnow(),
            'is_active': True
        }
        db.users.insert_one(admin_user)
        print("✅ Created test admin user: admin@siliguri.com / Admin123!")
    else:
        print("✅ Test admin user already exists")
    
    # ==================== CREATE SAMPLE ASSETS ====================
    print("\n🏭 Creating sample assets...")
    
    sample_substation = db.assets.find_one({'asset_id': 'SS-001'})
    if not sample_substation:
        substation = {
            'asset_id': 'SS-001',
            'asset_type': 'substation',
            'asset_name': 'Siliguri Town',
            'region': 'Darjeeling',
            'division': 'Siliguri Town',
            'location': 'City Center',
            'technical_specs': {
                'capacity_mva': 40,
                'ptr_count': 2,
                'ptr_details': '2×20 MVA PTRs',
                'commission_date': '2018-05-15'
            }
        }
        db.assets.insert_one(substation)
        print("✅ Created sample substation: Siliguri Town")
    
    print("\n" + "="*50)
    print("✅ MONGODB INITIALIZATION COMPLETE!")
    print("="*50)
    
    return True

if __name__ == '__main__':
    init_database()