"""
Authentication Routes - Using MongoDB Users (Synced from Google Sheet)
Users are synced from Google Sheet to MongoDB via sync_users.py
Last Updated: June 23, 2026
"""

from flask import Blueprint, render_template, request, session, flash, redirect, url_for
from app.utils.decorators import login_required
from app.models.mongo_utils import get_user_db
from datetime import datetime
import hashlib
import re

auth_bp = Blueprint('auth', __name__)

# ================================================================
# CONFIGURATION
# ================================================================
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_TIME_MINUTES = 15

# Fallback users (if MongoDB is not available)
FALLBACK_USERS = {
    'admin': {'password': 'admin123', 'role': 'admin', 'name': 'Administrator'},
    'manager': {'password': 'manager123', 'role': 'manager', 'name': 'Manager'},
    'user': {'password': 'user123', 'role': 'user', 'name': 'User'},
    'mofiz': {'password': 'Mofiz123', 'role': 'user', 'name': 'Mofiz'}
}

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def validate_username(username):
    """Validate username format"""
    if not username or len(username) < 3:
        return False, "Username must be at least 3 characters"
    if not re.match(r'^[a-zA-Z0-9_]+$', username):
        return False, "Username can only contain letters, numbers, and underscores"
    return True, ""

def hash_password(password):
    """Hash password for secure storage"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed):
    """Verify password against hashed version"""
    return hash_password(password) == hashed

def get_users_from_mongodb():
    """
    Get users from MongoDB (already synced from Google Sheet)
    Returns dictionary of username -> user_data
    """
    try:
        db = get_user_db()
        if db is None:
            print("❌ User database not connected")
            return None
        
        # Check if users collection exists
        collections = db.list_collection_names()
        if 'users' not in collections:
            print("⚠️ 'users' collection not found in MongoDB")
            return None
        
        users = {}
        for user in db.users.find({}, {'_id': 0}):
            username = user.get('username', '').lower()
            if username:
                # Get password (plain text from Google Sheet sync)
                password = user.get('password', '')
                
                # If password is hashed, verify with hash
                # For now, we're using plain text as synced from Google Sheet
                users[username] = {
                    'password': password,
                    'role': user.get('role', 'user'),
                    'name': user.get('name', username),
                    'active': user.get('active', True),
                    'email': user.get('email', ''),
                    'last_login': user.get('last_login', None)
                }
        
        print(f"✅ Loaded {len(users)} users from MongoDB")
        return users
        
    except Exception as e:
        print(f"⚠️ MongoDB user fetch failed: {e}")
        return None

def get_users():
    """
    Get users from MongoDB or fallback to default users
    Returns dictionary of username -> user_data
    """
    # Try MongoDB first
    users = get_users_from_mongodb()
    
    if users:
        # Print users for debugging (without passwords)
        for username, data in users.items():
            status = "✅ Active" if data.get('active', True) else "❌ Inactive"
            print(f"   👤 {username} → {data['role']} ({data['name']}) {status}")
        return users
    
    # Fallback to hardcoded users
    print(f"⚠️ Using {len(FALLBACK_USERS)} fallback users")
    return FALLBACK_USERS

def update_last_login(username):
    """Update user's last login timestamp"""
    try:
        db = get_user_db()
        if db is None:
            return
        
        db.users.update_one(
            {'username': username},
            {'$set': {'last_login': datetime.now()}}
        )
    except Exception as e:
        print(f"⚠️ Could not update last login: {e}")

def get_user_by_username(username):
    """Get single user by username from MongoDB"""
    try:
        db = get_user_db()
        if db is None:
            return None
        
        user = db.users.find_one({'username': username.lower()}, {'_id': 0})
        if user:
            return {
                'username': user.get('username'),
                'role': user.get('role', 'user'),
                'name': user.get('name', username),
                'active': user.get('active', True),
                'email': user.get('email', '')
            }
        return None
    except Exception as e:
        print(f"⚠️ Error fetching user: {e}")
        return None

# ================================================================
# ROUTES
# ================================================================

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Login page - username and password authentication"""
    # If already logged in, redirect to dashboard
    if 'user' in session:
        return redirect(url_for('main.general_overview'))
    
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        password = request.form.get('password', '').strip()
        remember = request.form.get('remember', False)
        
        # Validate username format
        is_valid, error_msg = validate_username(username)
        if not is_valid:
            flash(error_msg, 'danger')
            return render_template('login.html')
        
        # Get users from MongoDB
        users = get_users()
        
        # Validate credentials
        if username in users and users[username]['password'] == password:
            # Check if user is active
            if not users[username].get('active', True):
                flash('Your account is inactive. Please contact admin.', 'danger')
                return render_template('login.html')
            
            # Clear existing flash messages
            session.pop('_flashes', None)
            
            # Store user in session
            session['user'] = {
                'username': username,
                'role': users[username]['role'],
                'name': users[username]['name']
            }
            
            # Set session to permanent if remember me is checked
            if remember:
                session.permanent = True
            
            # Update last login
            update_last_login(username)
            
            flash(f'Welcome back, {users[username]["name"]}!', 'success')
            return redirect(url_for('main.general_overview'))
        else:
            flash('Invalid username or password', 'danger')
    
    return render_template('login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    """Logout user and clear session"""
    username = session.get('user', {}).get('username', 'Guest')
    session.clear()
    flash(f'Goodbye, {username}! You have been logged out.', 'info')
    return redirect(url_for('auth.login'))

@auth_bp.route('/profile')
@login_required
def profile():
    """User profile page - shows current user info"""
    user = session.get('user', {})
    
    # Get full user details from MongoDB
    user_data = get_user_by_username(user.get('username'))
    
    if user_data:
        user_info = {
            'username': user_data.get('username'),
            'name': user_data.get('name'),
            'role': user_data.get('role'),
            'email': user_data.get('email'),
            'active': user_data.get('active', True)
        }
    else:
        user_info = user
    
    return render_template('profile.html', user=user_info)

@auth_bp.route('/admin/users')
@login_required
def list_users():
    """List all users - Admin only"""
    # Check if user is admin
    user = session.get('user', {})
    if user.get('role') != 'admin':
        flash('Admin access required', 'danger')
        return redirect(url_for('main.general_overview'))
    
    users = get_users()
    return render_template('admin/users.html', users=users, user=user)

@auth_bp.route('/admin/refresh-users', methods=['POST'])
@login_required
def refresh_users():
    """Force refresh users from Google Sheet - Admin only"""
    # Check if user is admin
    user = session.get('user', {})
    if user.get('role') != 'admin':
        return {'success': False, 'message': 'Admin access required'}, 403
    
    try:
        from app.models.user_manager import get_user_manager
        
        user_manager = get_user_manager()
        if user_manager:
            user_manager.refresh()
            flash('Users refreshed from Google Sheet successfully!', 'success')
            return {'success': True, 'message': 'Users refreshed successfully'}
        else:
            flash('User manager not available', 'danger')
            return {'success': False, 'message': 'User manager not available'}, 500
    except Exception as e:
        flash(f'Error refreshing users: {e}', 'danger')
        return {'success': False, 'message': str(e)}, 500

# ================================================================
# CONTEXT PROCESSOR
# ================================================================

@auth_bp.context_processor
def inject_user():
    """Inject user into all auth templates"""
    return {'user': session.get('user')}