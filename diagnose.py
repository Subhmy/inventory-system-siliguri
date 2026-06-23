"""
Complete System Diagnostic
Run this to check everything at once
"""

import os
import sys
from dotenv import load_dotenv

MAIN_PROJECT_PATH = r"D:\inventory-system-siliguri-main"
sys.path.append(MAIN_PROJECT_PATH)

load_dotenv(os.path.join(MAIN_PROJECT_PATH, '.env'))

print("=" * 70)
print("🔍 COMPLETE SYSTEM DIAGNOSTIC")
print("=" * 70)

# ================================================================
# 1. CHECK ENVIRONMENT
# ================================================================
print("\n📋 1. ENVIRONMENT VARIABLES:")
print(f"   MONGO_DB_NAME: {os.getenv('MONGO_DB_NAME', 'NOT SET')}")
print(f"   INVENTORY_DB_NAME: {os.getenv('INVENTORY_DB_NAME', 'NOT SET')}")
print(f"   USER_DB_NAME: {os.getenv('USER_DB_NAME', 'NOT SET')}")
print(f"   USER_SHEET_ID: {os.getenv('USER_SHEET_ID', 'NOT SET')}")
print(f"   USER_SHEET_GID: {os.getenv('USER_SHEET_GID', 'NOT SET')}")

# ================================================================
# 2. CHECK DATABASE CONNECTION
# ================================================================
print("\n📊 2. DATABASE CONNECTION:")
try:
    from app.models.mongo_utils import get_db, get_inventory_db, get_user_db
    
    db = get_db()
    if db:
        print(f"   ✅ Default DB: {db.name}")
    else:
        print("   ❌ Default DB: Failed")
    
    inv_db = get_inventory_db()
    if inv_db:
        print(f"   ✅ Inventory DB: {inv_db.name}")
        collections = inv_db.list_collection_names()
        print(f"      Collections: {collections}")
    else:
        print("   ❌ Inventory DB: Failed")
    
    user_db = get_user_db()
    if user_db:
        print(f"   ✅ User DB: {user_db.name}")
        collections = user_db.list_collection_names()
        print(f"      Collections: {collections}")
    else:
        print("   ❌ User DB: Failed")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

# ================================================================
# 3. CHECK USERS
# ================================================================
print("\n👤 3. USER MANAGEMENT:")

# Check Google Sheet users
try:
    from app.models.user_manager import get_user_manager
    um = get_user_manager()
    if um:
        users = um.get_all_users()
        print(f"   ✅ Google Sheet Users: {len(users)}")
        for username, data in users.items():
            print(f"      {username} → {data['role']} ({data['name']})")
    else:
        print("   ⚠️ User Manager returned None")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Check MongoDB users
try:
    user_db = get_user_db()
    if user_db and 'users' in user_db.list_collection_names():
        mongo_users = list(user_db.users.find({}, {'_id': 0}))
        print(f"   ✅ MongoDB Users: {len(mongo_users)}")
        for user in mongo_users:
            print(f"      {user.get('username')} → {user.get('role')} ({user.get('name')})")
    else:
        print("   ⚠️ No users collection in MongoDB")
except Exception as e:
    print(f"   ❌ Error: {e}")

# ================================================================
# 4. CHECK AUTH MODULE
# ================================================================
print("\n🔑 4. AUTH MODULE:")
try:
    from app.routes.auth import get_users
    users = get_users()
    print(f"   ✅ get_users() returned {len(users)} users:")
    for username, data in users.items():
        print(f"      {username} → {data['role']}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# ================================================================
# 5. CHECK DECORATORS
# ================================================================
print("\n🛡️ 5. DECORATORS:")
try:
    from app.utils.decorators import login_required, role_required
    print("   ✅ login_required imported")
    print("   ✅ role_required imported")
except Exception as e:
    print(f"   ❌ Error: {e}")

# ================================================================
# 6. CHECK ROUTES
# ================================================================
print("\n🌐 6. ROUTES:")
try:
    from app import create_app
    app = create_app()
    
    inventory_routes = [
        '/general-overview',
        '/admin-overview',
        '/technical-overview',
        '/commercial-overview',
        '/priority-works-overview',
        '/inventory-dashboard',
        '/consumption-analysis',
        '/current-stock-position',
        '/material-in-transit'
    ]
    
    print("   ✅ Routes registered:")
    for route in inventory_routes:
        found = False
        for rule in app.url_map.iter_rules():
            if rule.rule == route:
                found = True
                break
        print(f"      {route}: {'✅ Found' if found else '❌ NOT FOUND'}")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

# ================================================================
# 7. CHECK TEMPLATES
# ================================================================
print("\n📁 7. TEMPLATES:")
template_dir = os.path.join(MAIN_PROJECT_PATH, 'templates')
templates = [
    'base.html',
    'login.html',
    'sidebar.html',
    'general_overview.html',
    'admin_overview.html',
    'technical_overview.html',
    'commercial_overview.html',
    'priority_works_overview.html',
    'inventory_dashboard.html'
]

for template in templates:
    path = os.path.join(template_dir, template)
    exists = os.path.exists(path)
    print(f"   {template}: {'✅ Found' if exists else '❌ MISSING'}")

# ================================================================
# 8. TEST SESSION
# ================================================================
print("\n🔐 8. SESSION TEST:")
print("   To test session, login and visit /debug/session")
print("   Expected for admin: {'username': 'admin', 'role': 'admin', 'name': 'Administrator'}")

print("\n" + "=" * 70)
print("✅ DIAGNOSTIC COMPLETE!")
print("=" * 70)