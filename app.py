"""
INVENTORY MANAGEMENT SYSTEM - SILIGURI ZONE
Main Application File - Production Ready
"""

import os
import sys
from flask import Flask, render_template, request, redirect, url_for, session, flash
from supabase import create_client
from dotenv import load_dotenv
from datetime import timedelta
from functools import wraps

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)

# Get secret key from environment or use default (for production)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'inventory-management-siliguri-2026-secret-key')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)

# Get Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_ANON_KEY')

# Validate environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment", file=sys.stderr)
    # Don't exit, but will fail on database calls

# Initialize Supabase
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Supabase client initialized successfully")
except Exception as e:
    print(f"ERROR initializing Supabase: {e}", file=sys.stderr)
    supabase = None

# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash('Please log in first', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Home page
@app.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

# Login page
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        if not supabase:
            flash('Database connection error. Please try again later.', 'danger')
            return render_template('login.html')
        
        try:
            # Attempt login with Supabase
            auth_response = supabase.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            
            if auth_response and auth_response.user:
                try:
                    # Get user's role from profiles table
                    profile = supabase.table('profiles')\
                        .select('role')\
                        .eq('id', auth_response.user.id)\
                        .execute()
                    
                    role = 'user'
                    if profile.data and len(profile.data) > 0:
                        role = profile.data[0]['role']
                except:
                    role = 'user'  # Default if profile query fails
                
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
            error_msg = str(e)
            if 'Invalid login credentials' in error_msg:
                flash('Invalid email or password', 'danger')
            else:
                flash(f'Login error. Please try again.', 'danger')
                print(f"Login error: {error_msg}", file=sys.stderr)
    
    return render_template('login.html')

# Dashboard
@app.route('/dashboard')
@login_required
def dashboard():
    user = session['user']
    return render_template('dashboard.html', user=user)

# Test role page
@app.route('/my-role')
@login_required
def my_role():
    user = session['user']
    return f"Email: {user['email']}<br>Role: {user['role']}<br><a href='/dashboard'>Back</a>"

# Logout
@app.route('/logout')
def logout():
    try:
        if supabase:
            supabase.auth.sign_out()
    except:
        pass
    session.clear()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))

# Health check endpoint for Render
@app.route('/healthz')
def healthz():
    return "OK", 200

# Error handlers
@app.errorhandler(500)
def internal_error(e):
    return render_template('login.html'), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)