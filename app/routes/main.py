"""
Main Routes - Dashboard Pages with Role Protection
Enhanced with error handling, logging, and debug support
Last Updated: June 19, 2026
"""

from flask import Blueprint, redirect, url_for, render_template, session, flash, request, jsonify
from app.utils.decorators import login_required, role_required
import traceback
import sys

main_bp = Blueprint('main', __name__)

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def get_user():
    """Get current user from session with safe fallback"""
    return session.get('user', {
        'username': 'Guest',
        'role': 'user',
        'name': 'Guest'
    })

def log_access(route_name, user):
    """Log user access for debugging"""
    print(f"🔐 ACCESS: {user.get('username', 'Guest')} ({user.get('role', 'user')}) → {route_name}")

def handle_error(route_name, error):
    """Handle errors gracefully"""
    print(f"❌ ERROR in {route_name}: {error}")
    traceback.print_exc()
    flash(f'An error occurred while loading {route_name}. Please try again.', 'danger')

# ================================================================
# MAIN ROUTES
# ================================================================

@main_bp.route('/')
def index():
    """Root route - redirect to login"""
    return redirect(url_for('auth.login'))

# ================================================================
# GENERAL OVERVIEW - ALL USERS
# ================================================================

@main_bp.route('/general-overview')
@login_required
def general_overview():
    """General Overview dashboard - Visible to ALL users"""
    user = get_user()
    log_access('general-overview', user)
    return render_template('general_overview.html', user=user)

# ================================================================
# ADMIN ONLY ROUTES
# ================================================================

@main_bp.route('/admin-overview')
@login_required
@role_required('admin')
def admin_overview():
    """Administrative Dashboard - ADMIN ONLY"""
    user = get_user()
    log_access('admin-overview', user)
    try:
        return render_template('admin_overview.html', user=user)
    except Exception as e:
        handle_error('admin-overview', e)
        flash('Admin dashboard is currently unavailable. Please check the server logs.', 'danger')
        return redirect(url_for('main.general_overview'))

# ================================================================
# ADMIN & MANAGER ONLY ROUTES
# ================================================================

@main_bp.route('/technical-overview')
@login_required
@role_required('admin', 'manager')
def technical_overview():
    """Technical Overview dashboard - ADMIN & MANAGER ONLY"""
    user = get_user()
    log_access('technical-overview', user)
    try:
        return render_template('technical_overview.html', user=user)
    except Exception as e:
        handle_error('technical-overview', e)
        flash('Technical dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/commercial-overview')
@login_required
@role_required('admin', 'manager')
def commercial_overview():
    """Commercial Overview dashboard - ADMIN & MANAGER ONLY"""
    user = get_user()
    log_access('commercial-overview', user)
    try:
        return render_template('commercial_overview.html', user=user)
    except Exception as e:
        handle_error('commercial-overview', e)
        flash('Commercial dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/priority-works-overview')
@login_required
@role_required('admin', 'manager')
def priority_works_overview():
    """Priority Works Overview dashboard - ADMIN & MANAGER ONLY"""
    user = get_user()
    log_access('priority-works-overview', user)
    try:
        return render_template('priority_works_overview.html', user=user)
    except Exception as e:
        handle_error('priority-works-overview', e)
        flash('Priority Works dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

# ================================================================
# INVENTORY MANAGEMENT DASHBOARDS - ALL USERS
# ================================================================

@main_bp.route('/inventory-dashboard')
@login_required
def inventory_dashboard():
    """Inventory Management Dashboard - Stock, Critical Items, Consumption, Allotment"""
    user = get_user()
    log_access('inventory-dashboard', user)
    try:
        return render_template('inventory_dashboard.html', user=user)
    except Exception as e:
        handle_error('inventory-dashboard', e)
        flash('Inventory dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/consumption-analysis')
@login_required
def consumption_analysis():
    """Consumption Analysis Detail Dashboard"""
    user = get_user()
    log_access('consumption-analysis', user)
    try:
        return render_template('dashboards/consumption_analysis_new.html', user=user)
    except Exception as e:
        handle_error('consumption-analysis', e)
        flash('Consumption Analysis dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/current-stock-position')
@login_required
def current_stock_position():
    """Current Stock Position Dashboard"""
    user = get_user()
    log_access('current-stock-position', user)
    try:
        return render_template('dashboards/current_stock_position.html', user=user)
    except Exception as e:
        handle_error('current-stock-position', e)
        flash('Current Stock dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/material-in-transit')
@login_required
def material_in_transit():
    """Material in Transit Dashboard - Tracks DI and STN shipments"""
    user = get_user()
    log_access('material-in-transit', user)
    try:
        return render_template('dashboards/material_in_transit.html', user=user)
    except Exception as e:
        handle_error('material-in-transit', e)
        flash('Material in Transit dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

# ================================================================
# TECHNICAL DASHBOARD ROUTES - ADMIN & MANAGER ONLY
# ================================================================

@main_bp.route('/substation-dashboard')
@login_required
@role_required('admin', 'manager')
def substation_dashboard():
    """33/11 KV Sub-Station main dashboard"""
    user = get_user()
    log_access('substation-dashboard', user)
    try:
        return render_template('dashboards/substation_33_11kv_dashboard.html', user=user)
    except Exception as e:
        handle_error('substation-dashboard', e)
        flash('Substation dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/line-33kv-dashboard')
@login_required
@role_required('admin', 'manager')
def line_33kv_dashboard():
    """33 KV Line main dashboard"""
    user = get_user()
    log_access('line-33kv-dashboard', user)
    try:
        return render_template('dashboards/line_33kv_dashboard.html', user=user)
    except Exception as e:
        handle_error('line-33kv-dashboard', e)
        flash('33KV Line dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/line-11kv-dashboard')
@login_required
@role_required('admin', 'manager')
def line_11kv_dashboard():
    """11 KV Line main dashboard"""
    user = get_user()
    log_access('line-11kv-dashboard', user)
    try:
        return render_template('dashboards/line_11kv_dashboard.html', user=user)
    except Exception as e:
        handle_error('line-11kv-dashboard', e)
        flash('11KV Line dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

# ================================================================
# PRIORITY WORKS DASHBOARD ROUTES - ADMIN & MANAGER ONLY
# ================================================================

@main_bp.route('/hvds-dashboard')
@login_required
@role_required('admin', 'manager')
def hvds_dashboard():
    """HVDS main dashboard"""
    user = get_user()
    log_access('hvds-dashboard', user)
    try:
        return render_template('dashboards/hvds_dashboard.html', user=user)
    except Exception as e:
        handle_error('hvds-dashboard', e)
        flash('HVDS dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/new-substation-dashboard')
@login_required
@role_required('admin', 'manager')
def new_substation_dashboard():
    """New 33/11 KV Sub-Station dashboard"""
    user = get_user()
    log_access('new-substation-dashboard', user)
    try:
        return render_template('dashboards/New_33_11kv_Sub-Stn_dashboard.html', user=user)
    except Exception as e:
        handle_error('new-substation-dashboard', e)
        flash('New Sub-Station dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/ptr-augmentation-dashboard')
@login_required
@role_required('admin', 'manager')
def ptr_augmentation_dashboard():
    """PTR Augmentation dashboard"""
    user = get_user()
    log_access('ptr-augmentation-dashboard', user)
    try:
        return render_template('dashboards/PTR_Augmentation_dashboard.html', user=user)
    except Exception as e:
        handle_error('ptr-augmentation-dashboard', e)
        flash('PTR Augmentation dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/new-33kv-line-dashboard')
@login_required
@role_required('admin', 'manager')
def new_33kv_line_dashboard():
    """New 33KV Line dashboard"""
    user = get_user()
    log_access('new-33kv-line-dashboard', user)
    try:
        return render_template('dashboards/New_33KV_Line_dashboard.html', user=user)
    except Exception as e:
        handle_error('new-33kv-line-dashboard', e)
        flash('New 33KV Line dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/conductor-augmt-33kv-dashboard')
@login_required
@role_required('admin', 'manager')
def conductor_augmt_33kv_dashboard():
    """33KV Conductor Augmentation dashboard"""
    user = get_user()
    log_access('conductor-augmt-33kv-dashboard', user)
    try:
        return render_template('dashboards/Conductor_Augmt_33KV_Line_dashboard.html', user=user)
    except Exception as e:
        handle_error('conductor-augmt-33kv-dashboard', e)
        flash('33KV Conductor Augmentation dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/new-11kv-line-dashboard')
@login_required
@role_required('admin', 'manager')
def new_11kv_line_dashboard():
    """New 11KV Line dashboard"""
    user = get_user()
    log_access('new-11kv-line-dashboard', user)
    try:
        return render_template('dashboards/New_11KV_11_Line_dashboard.html', user=user)
    except Exception as e:
        handle_error('new-11kv-line-dashboard', e)
        flash('New 11KV Line dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

@main_bp.route('/conductor-augmt-11kv-dashboard')
@login_required
@role_required('admin', 'manager')
def conductor_augmt_11kv_dashboard():
    """11KV Conductor Augmentation dashboard"""
    user = get_user()
    log_access('conductor-augmt-11kv-dashboard', user)
    try:
        return render_template('dashboards/Conductor_Augmt_11_Line_dashboard.html', user=user)
    except Exception as e:
        handle_error('conductor-augmt-11kv-dashboard', e)
        flash('11KV Conductor Augmentation dashboard is currently unavailable.', 'danger')
        return redirect(url_for('main.general_overview'))

# ================================================================
# DEBUG & TEST ROUTES
# ================================================================

@main_bp.route('/debug/session')
@login_required
def debug_session():
    """Debug route to check session data"""
    user = get_user()
    user_role = user.get('role', '')
    return jsonify({
        'session': dict(session),
        'user': user,
        'is_admin': user_role == 'admin',
        'is_manager': user_role in ['admin', 'manager'],
        'is_user': user_role in ['user', 'admin', 'manager']
    })

@main_bp.route('/debug/role-check')
@login_required
def debug_role_check():
    """Check if role-based access is working"""
    user = get_user()
    user_role = user.get('role', '')
    return jsonify({
        'user': user,
        'can_access_admin': user_role == 'admin',
        'can_access_manager': user_role in ['admin', 'manager'],
        'can_access_user': user_role in ['admin', 'manager', 'user']
    })

@main_bp.route('/test')
@login_required
def test_page():
    """Simple test page to verify routing works"""
    user = get_user()
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Page</title>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 40px; background: #f0f2f5; }}
            .card {{ background: white; border-radius: 12px; padding: 30px; max-width: 600px; margin: auto; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
            h1 {{ color: #28a745; }}
            .info {{ background: #f8fafd; padding: 15px; border-radius: 8px; margin: 15px 0; }}
            .links {{ display: flex; flex-direction: column; gap: 8px; }}
            .links a {{ color: #667eea; text-decoration: none; }}
            .links a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>✅ Test Page - Routing Works!</h1>
            <div class="info">
                <p><strong>User:</strong> {user.get('username')}</p>
                <p><strong>Role:</strong> {user.get('role')}</p>
                <p><strong>Name:</strong> {user.get('name')}</p>
            </div>
            <hr>
            <div class="links">
                <a href="/general-overview">📊 General Overview</a>
                <a href="/inventory-dashboard">📦 Inventory Dashboard</a>
                <a href="/debug/session">🔍 Debug Session</a>
                <a href="/debug/role-check">🔐 Debug Role Check</a>
                <a href="/admin-overview">👤 Admin Overview</a>
                <a href="/technical-overview">⚙️ Technical Overview</a>
            </div>
        </div>
    </body>
    </html>
    """

# ================================================================
# ERROR HANDLERS
# ================================================================

@main_bp.errorhandler(404)
def page_not_found(e):
    """404 error handler"""
    user = get_user()
    print(f"❌ 404: {request.url}")
    return render_template('errors/404.html', user=user), 404

@main_bp.errorhandler(500)
def internal_server_error(e):
    """500 error handler with detailed logging"""
    user = get_user()
    print(f"❌ 500 ERROR: {e}")
    print(traceback.format_exc())
    return render_template('errors/500.html', user=user), 500

@main_bp.errorhandler(403)
def forbidden(e):
    """403 error handler - access denied"""
    user = get_user()
    flash('You do not have permission to access this page.', 'danger')
    return redirect(url_for('main.general_overview'))

# ================================================================
# CONTEXT PROCESSOR - Make user available to all templates
# ================================================================

@main_bp.context_processor
def inject_user():
    """Inject user into all templates"""
    return {'user': get_user()}