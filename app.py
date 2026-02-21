"""
INVENTORY MANAGEMENT SYSTEM - SILIGURI ZONE
COMPLETELY FIXED - Forces Login First
"""

import os
from flask import Flask, render_template, request, redirect, url_for, session, flash
from supabase import create_client
from dotenv import load_dotenv
from datetime import timedelta
from functools import wraps

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)
app.secret_key = 'inventory-management-siliguri-2026-secret-key'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)

# Get Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_ANON_KEY')

# Initialize Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================== LOGIN REQUIRED DECORATOR ====================
def login_required(f):
    """
    Decorator to protect pages that need login
    If user not in session, redirect to login
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash('Please log in first', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ==================== ROUTES ====================

@app.route('/')
def index():
    """HOME PAGE - ALWAYS go to login first"""
    # Clear any existing session to be safe
    if 'user' in session:
        # If somehow logged in, go to dashboard
        return redirect(url_for('dashboard'))
    # Otherwise go to login
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page - FIRST PAGE users see"""
    # If already logged in, go to dashboard
    if 'user' in session:
        return redirect(url_for('dashboard'))
    
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        try:
            # Attempt login with Supabase
            auth_response = supabase.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            
            if auth_response.user:
                # Get user's role from profiles table
                profile = supabase.table('profiles')\
                    .select('role')\
                    .eq('id', auth_response.user.id)\
                    .execute()
                
                role = 'user'
                if profile.data and len(profile.data) > 0:
                    role = profile.data[0]['role']
                
                # Store user in session
                session['user'] = {
                    'id': auth_response.user.id,
                    'email': auth_response.user.email,
                    'role': role
                }
                session.permanent = True
                
                flash(f'Welcome back, {email}!', 'success')
                return redirect(url_for('dashboard'))
            else:
                flash('Invalid email or password', 'danger')
                
        except Exception as e:
            error_message = str(e)
            if 'Invalid login credentials' in error_message:
                flash('Invalid email or password', 'danger')
            else:
                flash(f'Login error: {error_message}', 'danger')
    
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard - ONLY accessible when logged in"""
    user = session['user']
    return render_template('dashboard.html', user=user)

@app.route('/logout')
def logout():
    """Log out user"""
    try:
        supabase.auth.sign_out()
    except:
        pass
    # Clear session completely
    session.clear()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))

# ==================== TEST ROUTE (Remove after testing) ====================
@app.route('/test-session')
def test_session():
    """Test route to check if session exists"""
    if 'user' in session:
        return f"Logged in as: {session['user']['email']}"
    return "Not logged in"

# ==================== RUN APPLICATION ====================
if __name__ == '__main__':
    app.run(debug=True)