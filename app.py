"""
INVENTORY MANAGEMENT SYSTEM - SILIGURI ZONE
Main Application File - Complete Version
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
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'inventory-management-siliguri-secret-key-2026')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)

# Get Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_ANON_KEY')

# Initialize Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================== HELPER FUNCTIONS ====================
def login_required(f):
    """Decorator to require login for certain pages"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash('Please log in first', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ==================== AUTHENTICATION ROUTES ====================

@app.route('/')
def index():
    """Home page - redirect to login or dashboard"""
    if 'user' in session:
        return redirect(url_for('priority_works'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page - users can only log in, not register"""
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
                return redirect(url_for('priority_works'))
            else:
                flash('Invalid email or password', 'danger')
                
        except Exception as e:
            error_message = str(e)
            if 'Invalid login credentials' in error_message:
                flash('Invalid email or password', 'danger')
            else:
                flash(f'Login error: {error_message}', 'danger')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Log out user"""
    try:
        supabase.auth.sign_out()
    except:
        pass
    session.clear()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))

# ==================== DASHBOARD ROUTES ====================

@app.route('/priority-works')
@login_required
def priority_works():
    """Priority Works home page"""
    user = session['user']
    return render_template('dashboards/priority_works_home.html', user=user)

# ==================== 33/11 KV SUB-STATION ROUTES ====================

@app.route('/substation-dashboard')
@login_required
def substation_dashboard():
    """33/11 KV Sub-Station main dashboard"""
    user = session['user']
    return render_template('dashboards/substation_33_11kv_dashboard.html', user=user)

@app.route('/substation-ptr')
@login_required
def substation_ptr():
    """33/11 KV Sub-Station PTR Work"""
    user = session['user']
    return render_template('dashboards/substation_33_11kv_ptr.html', user=user)

@app.route('/substation-bus')
@login_required
def substation_bus():
    """33/11 KV Sub-Station Bus Work"""
    user = session['user']
    return render_template('dashboards/substation_33_11kv_bus.html', user=user)

@app.route('/substation-new')
@login_required
def substation_new():
    """Create New 33/11 KV Sub-Station Project"""
    user = session['user']
    return render_template('dashboards/substation_33_11kv_new.html', user=user)

# ==================== HVDS ROUTES ====================

@app.route('/hvds-dashboard')
@login_required
def hvds_dashboard():
    """HVDS main dashboard"""
    user = session['user']
    return render_template('dashboards/hvds_dashboard.html', user=user)

@app.route('/hvds-100kva')
@login_required
def hvds_100kva():
    """HVDS 100 KVA DTR Work"""
    user = session['user']
    return render_template('dashboards/hvds_dtr_100kva.html', user=user)

@app.route('/hvds-63kva')
@login_required
def hvds_63kva():
    """HVDS 63 KVA DTR Work"""
    user = session['user']
    return render_template('dashboards/hvds_dtr_63kva.html', user=user)

@app.route('/hvds-25kva')
@login_required
def hvds_25kva():
    """HVDS 25 KVA DTR Work"""
    user = session['user']
    return render_template('dashboards/hvds_dtr_25kva.html', user=user)

# ==================== 33 KV LINE ROUTES ====================

@app.route('/line-33kv-dashboard')
@login_required
def line_33kv_dashboard():
    """33 KV Line main dashboard"""
    user = session['user']
    return render_template('dashboards/line_33kv_dashboard.html', user=user)

@app.route('/line-33kv-new')
@login_required
def line_33kv_new():
    """New 33 KV Line Project"""
    user = session['user']
    return render_template('dashboards/line_33kv_new.html', user=user)

@app.route('/line-33kv-conductor')
@login_required
def line_33kv_conductor():
    """33 KV Line Conductor Work"""
    user = session['user']
    return render_template('dashboards/line_33kv_conductor.html', user=user)

# ==================== 11 KV LINE ROUTES ====================

@app.route('/line-11kv-dashboard')
@login_required
def line_11kv_dashboard():
    """11 KV Line main dashboard"""
    user = session['user']
    return render_template('dashboards/line_11kv_dashboard.html', user=user)

@app.route('/line-11kv-new')
@login_required
def line_11kv_new():
    """New 11 KV Line Project"""
    user = session['user']
    return render_template('dashboards/line_11kv_new.html', user=user)

@app.route('/line-11kv-feeder')
@login_required
def line_11kv_feeder():
    """11 KV Line Feeder Work"""
    user = session['user']
    return render_template('dashboards/line_11kv_feeder.html', user=user)

@app.route('/line-11kv-conductor')
@login_required
def line_11kv_conductor():
    """11 KV Line Conductor Work"""
    user = session['user']
    return render_template('dashboards/line_11kv_conductor.html', user=user)

# ==================== TEST ROUTES ====================

@app.route('/my-role')
@login_required
def my_role():
    """Simple page to check user role (for testing)"""
    user = session['user']
    return f"""
    <h2>Your Information</h2>
    <p>Email: {user['email']}</p>
    <p>Role: {user['role']}</p>
    <p>User ID: {user['id']}</p>
    <br>
    <a href="/priority-works">Back to Dashboard</a>
    """

# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def page_not_found(e):
    """Handle 404 errors"""
    flash('Page not found', 'warning')
    if 'user' in session:
        return redirect(url_for('priority_works'))
    return redirect(url_for('login'))

@app.errorhandler(500)
def internal_server_error(e):
    """Handle 500 errors"""
    flash('Server error. Please try again.', 'danger')
    if 'user' in session:
        return redirect(url_for('priority_works'))
    return redirect(url_for('login'))

# ==================== HEALTH CHECK (for Render) ====================

@app.route('/healthz')
def healthz():
    """Health check endpoint for Render"""
    return "OK", 200

# ==================== RUN APPLICATION ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)