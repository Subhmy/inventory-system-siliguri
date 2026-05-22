from flask import Blueprint, redirect, url_for, render_template, session
from app.utils.decorators import login_required

main_bp = Blueprint('main', __name__)

# ==================== MAIN ROUTES ====================

@main_bp.route('/')
def index():
    """Root route - redirect to login"""
    return redirect(url_for('auth.login'))

@main_bp.route('/general-overview')
@login_required
def general_overview():
    """General Overview dashboard"""
    user = session.get('user')
    return render_template('general_overview.html', user=user)

@main_bp.route('/admin-overview')
@login_required
def admin_overview():
    """Administrative Dashboard"""
    user = session.get('user')
    return render_template('admin_overview.html', user=user)

@main_bp.route('/technical-overview')
@login_required
def technical_overview():
    """Technical Overview dashboard"""
    user = session.get('user')
    return render_template('technical_overview.html', user=user)

@main_bp.route('/commercial-overview')
@login_required
def commercial_overview():
    """Commercial Overview dashboard"""
    user = session.get('user')
    return render_template('commercial_overview.html', user=user)

@main_bp.route('/priority-works-overview')
@login_required
def priority_works_overview():
    """Priority Works Overview dashboard"""
    user = session.get('user')
    return render_template('priority_works_overview.html', user=user)

# ==================== INVENTORY MANAGEMENT DASHBOARD ====================

@main_bp.route('/inventory-dashboard')
@login_required
def inventory_dashboard():
    """Inventory Management Dashboard - Stock, Critical Items, Consumption, Allotment"""
    user = session.get('user')
    return render_template('inventory_dashboard.html', user=user)

# ==================== CONSUMPTION ANALYSIS DETAIL DASHBOARD ====================

@main_bp.route('/consumption-analysis')
@login_required
def consumption_analysis():
    """Consumption Analysis Detail Dashboard"""
    user = session.get('user')
    return render_template('dashboards/consumption_analysis_new.html', user=user)

# ==================== CURRENT STOCK POSITION DASHBOARD ====================

@main_bp.route('/current-stock-position')
@login_required
def current_stock_position():
    """Current Stock Position Dashboard"""
    user = session.get('user')
    return render_template('dashboards/current_stock_position.html', user=user)

# ==================== TECHNICAL DASHBOARD ROUTES ====================

@main_bp.route('/substation-dashboard')
@login_required
def substation_dashboard():
    """33/11 KV Sub-Station main dashboard"""
    user = session.get('user')
    return render_template('dashboards/substation_33_11kv_dashboard.html', user=user)

@main_bp.route('/line-33kv-dashboard')
@login_required
def line_33kv_dashboard():
    """33 KV Line main dashboard"""
    user = session.get('user')
    return render_template('dashboards/line_33kv_dashboard.html', user=user)

@main_bp.route('/line-11kv-dashboard')
@login_required
def line_11kv_dashboard():
    """11 KV Line main dashboard"""
    user = session.get('user')
    return render_template('dashboards/line_11kv_dashboard.html', user=user)

# ==================== PRIORITY WORKS DASHBOARD ROUTES ====================

@main_bp.route('/hvds-dashboard')
@login_required
def hvds_dashboard():
    """HVDS main dashboard"""
    user = session.get('user')
    return render_template('dashboards/hvds_dashboard.html', user=user)

@main_bp.route('/new-substation-dashboard')
@login_required
def new_substation_dashboard():
    """New 33/11 KV Sub-Station dashboard"""
    user = session.get('user')
    return render_template('dashboards/New_33_11kv_Sub-Stn_dashboard.html', user=user)

@main_bp.route('/ptr-augmentation-dashboard')
@login_required
def ptr_augmentation_dashboard():
    """PTR Augmentation dashboard"""
    user = session.get('user')
    return render_template('dashboards/PTR_Augmentation_dashboard.html', user=user)

@main_bp.route('/new-33kv-line-dashboard')
@login_required
def new_33kv_line_dashboard():
    """New 33KV Line dashboard"""
    user = session.get('user')
    return render_template('dashboards/New_33KV_Line_dashboard.html', user=user)

@main_bp.route('/conductor-augmt-33kv-dashboard')
@login_required
def conductor_augmt_33kv_dashboard():
    """33KV Conductor Augmentation dashboard"""
    user = session.get('user')
    return render_template('dashboards/Conductor_Augmt_33KV_Line_dashboard.html', user=user)

@main_bp.route('/new-11kv-line-dashboard')
@login_required
def new_11kv_line_dashboard():
    """New 11KV Line dashboard"""
    user = session.get('user')
    return render_template('dashboards/New_11KV_11_Line_dashboard.html', user=user)

@main_bp.route('/conductor-augmt-11kv-dashboard')
@login_required
def conductor_augmt_11kv_dashboard():
    """11KV Conductor Augmentation dashboard"""
    user = session.get('user')
    return render_template('dashboards/Conductor_Augmt_11_Line_dashboard.html', user=user)

# ==================== TEST ROUTE ====================

@main_bp.route('/test-consumption')
@login_required
def test_consumption():
    return "<h1>Test Page - Consumption Analysis Works!</h1>"