"""
INVENTORY MANAGEMENT SYSTEM - SILIGURI ZONE
Main Application File with MongoDB Integration
Last Updated: March 2, 2026
Enhanced with General Overview, Admin Dashboard, Commercial Dashboard and Export APIs
ADDED: MongoDB endpoints for Admin Dashboard
FIXED: All database connection checks
"""

import os
import csv
import json
from io import StringIO, BytesIO
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, make_response
from datetime import timedelta, datetime
from functools import wraps
from dotenv import load_dotenv
import pandas as pd

# Import MongoDB helper
from mongo_utils import ProjectDB, ReferenceDB, FilterDB, get_db, init_master_data

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'inventory-management-siliguri-secret-key-2026')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)

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

def get_current_user():
    """Get current user from session"""
    return session.get('user')

def format_currency(value):
    """Format value as currency in Cr/Lakhs"""
    if value >= 100:
        return f"₹{value:.1f} Cr"
    elif value >= 1:
        return f"₹{value:.1f} L"
    else:
        return f"₹{value*100:.0f} L"

def format_mu(value):
    """Format value in MU (Million Units)"""
    return f"{value:.1f} MU"

# ==================== AUTHENTICATION ROUTES ====================

@app.route('/')
def index():
    """Home page - redirect to login or general overview"""
    if 'user' in session:
        return redirect(url_for('general_overview'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page with hardcoded users"""
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        # Hardcoded users for simplicity
        users = {
            'admin@siliguri.com': {'password': 'admin123', 'role': 'admin'},
            'manager@siliguri.com': {'password': 'manager123', 'role': 'manager'},
            'user@siliguri.com': {'password': 'user123', 'role': 'user'}
        }
        
        if email in users and users[email]['password'] == password:
            # Get user info from database if available
            db = get_db()
            user_data = None
            
            # FIXED: Compare with None instead of 'if db:'
            if db is not None:
                user_data = db.users.find_one({"email": email})
            
            # Store user info in session
            session['user'] = {
                'id': user_data.get('_id', f'user_{email.split("@")[0]}') if user_data else f'user_{email.split("@")[0]}',
                'email': email,
                'role': users[email]['role']
            }
            session.permanent = True
            
            flash(f'Welcome back, {email}!', 'success')
            return redirect(url_for('general_overview'))
        else:
            flash('Invalid email or password', 'danger')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Log out user"""
    session.clear()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))

# ==================== GENERAL OVERVIEW ROUTES ====================

@app.route('/general-overview')
@login_required
def general_overview():
    """General Overview container page with dynamic filters"""
    user = session.get('user')
    return render_template('general_overview.html', user=user)

@app.route('/admin-overview')
@login_required
def admin_overview():
    """Administrative Dashboard - Region, Division & Staff Management"""
    user = session.get('user')
    return render_template('admin_overview.html', user=user)

@app.route('/commercial-overview')
@login_required
def commercial_overview():
    """Commercial Dashboard - Financial, Billing & Collection Metrics"""
    user = session.get('user')
    return render_template('commercial_overview.html', user=user)

@app.route('/zone-overview')
@login_required
def zone_overview():
    """Technical Dashboard - Infrastructure & Assets"""
    user = session.get('user')
    
    # Get all regions with their stats
    regions = FilterDB.get_all_regions()
    
    return render_template('zone_overview.html', user=user, regions=regions)

# ==================== API ROUTES FOR DYNAMIC FILTERS ====================

@app.route('/api/filter/regions', methods=['GET'])
@login_required
def api_get_filter_regions():
    """Get all regions for filters"""
    try:
        regions = FilterDB.get_all_regions()
        return jsonify({'regions': regions}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter/divisions', methods=['POST'])
@login_required
def api_get_filter_divisions():
    """Get divisions by region for cascading filters"""
    try:
        data = request.json
        region_id = data.get('region_id')
        
        if not region_id or region_id == 'all':
            # Return all divisions if no region specified
            db = get_db()
            # FIXED: Compare with None
            if db is not None:
                divisions = list(db.divisions.find({}))
                result = [{'id': d['_id'], 'name': d['name']} for d in divisions]
            else:
                result = []
        else:
            divisions = FilterDB.get_divisions_by_region(region_id)
            result = [{'id': d['id'], 'name': d['name']} for d in divisions]
        
        return jsonify({'divisions': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter/substations', methods=['POST'])
@login_required
def api_get_filter_substations():
    """Get substations by division for cascading filters"""
    try:
        data = request.json
        division_id = data.get('division_id')
        
        if not division_id or division_id == 'all':
            return jsonify({'substations': []}), 200
        
        substations = FilterDB.get_substations_by_division(division_id)
        result = [{'id': s['id'], 'name': s['name']} for s in substations]
        
        return jsonify({'substations': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== NEW MONGODB API ROUTES FOR ADMIN DASHBOARD ====================

@app.route('/api/zones')
@login_required
def api_zones():
    """Get all zones (from MongoDB centers collection)"""
    try:
        db = get_db()
        
        # FIXED: Check if db is None
        if db is None:
            # Return default if no database connection
            return jsonify([{
                "_id": "zone_siliguri",
                "name": "Siliguri Zone",
                "incharge": "Chief Engineer",
                "total_consumers": 0,
                "total_staff": 0,
                "total_dtr": 0,
                "center_count": 0
            }]), 200
        
        # Get all centers and calculate zone totals
        centers = list(db.centers.find({}))
        
        if centers:
            # Calculate totals from all centers
            total_consumers = sum(c.get('total_consumers', 0) for c in centers)
            total_staff = sum(c.get('total_staff', 0) for c in centers)
            total_dtr = sum(c.get('total_dtr', 0) for c in centers)
            
            zones = [{
                "_id": "zone_siliguri",
                "name": "Siliguri Zone",
                "incharge": "Chief Engineer",
                "total_consumers": total_consumers,
                "total_staff": total_staff,
                "total_dtr": total_dtr,
                "center_count": len(centers)
            }]
        else:
            # Return default if no data
            zones = [{
                "_id": "zone_siliguri",
                "name": "Siliguri Zone",
                "incharge": "Chief Engineer",
                "total_consumers": 0,
                "total_staff": 0,
                "total_dtr": 0,
                "center_count": 0
            }]
            
        return jsonify(zones), 200
    except Exception as e:
        print(f"Error in /api/zones: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/regions')
@login_required
def api_regions():
    """Get all regions with cumulative totals from centers"""
    try:
        db = get_db()
        
        # FIXED: Check if db is None
        if db is None:
            return jsonify([]), 200
        
        # Get all centers
        centers = list(db.centers.find({}))
        
        if not centers:
            return jsonify([]), 200
            
        # Group by region and calculate totals
        region_dict = {}
        for center in centers:
            region_name = center.get('region')
            if not region_name:
                continue
                
            if region_name not in region_dict:
                region_dict[region_name] = {
                    "name": region_name,
                    "total_consumers": 0,
                    "total_staff": 0,
                    "total_dtr": 0,
                    "center_count": 0,
                    "divisions": set()
                }
            
            region_dict[region_name]["total_consumers"] += center.get('total_consumers', 0)
            region_dict[region_name]["total_staff"] += center.get('total_staff', 0)
            region_dict[region_name]["total_dtr"] += center.get('total_dtr', 0)
            region_dict[region_name]["center_count"] += 1
            
            if center.get('division'):
                region_dict[region_name]["divisions"].add(center.get('division'))
        
        # Format response
        regions = []
        for i, (region_name, data) in enumerate(region_dict.items()):
            regions.append({
                "id": f"reg_{region_name.lower().replace(' ', '_')}",
                "name": region_name,
                "total_consumers": data["total_consumers"],
                "total_staff": data["total_staff"],
                "total_dtr": data["total_dtr"],
                "center_count": data["center_count"],
                "division_count": len(data["divisions"]),
                "color_index": (i % 4) + 1
            })
        
        # Sort by name
        regions.sort(key=lambda x: x['name'])
        
        return jsonify(regions), 200
    except Exception as e:
        print(f"Error in /api/regions: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/divisions')
@login_required
def api_divisions():
    """Get all divisions with cumulative totals from centers"""
    try:
        db = get_db()
        
        # FIXED: Check if db is None
        if db is None:
            return jsonify([]), 200
        
        # Get all centers
        centers = list(db.centers.find({}))
        
        if not centers:
            return jsonify([]), 200
            
        # Group by division and region
        division_dict = {}
        for center in centers:
            division_name = center.get('division')
            region_name = center.get('region')
            
            if not division_name or not region_name:
                continue
                
            key = f"{division_name}|{region_name}"
            if key not in division_dict:
                division_dict[key] = {
                    "name": division_name,
                    "region": region_name,
                    "total_consumers": 0,
                    "total_staff": 0,
                    "total_dtr": 0,
                    "center_count": 0
                }
            
            division_dict[key]["total_consumers"] += center.get('total_consumers', 0)
            division_dict[key]["total_staff"] += center.get('total_staff', 0)
            division_dict[key]["total_dtr"] += center.get('total_dtr', 0)
            division_dict[key]["center_count"] += 1
        
        # Format response
        divisions = []
        for data in division_dict.values():
            divisions.append({
                "id": f"div_{data['name'].lower().replace(' ', '_')}",
                "name": data["name"],
                "region": data["region"],
                "total_consumers": data["total_consumers"],
                "total_staff": data["total_staff"],
                "total_dtr": data["total_dtr"],
                "center_count": data["center_count"]
            })
        
        # Sort by region then name
        divisions.sort(key=lambda x: (x['region'], x['name']))
        
        return jsonify(divisions), 200
    except Exception as e:
        print(f"Error in /api/divisions: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/centers')
@login_required
def api_centers():
    """Get all centers with their data"""
    try:
        db = get_db()
        
        # FIXED: Check if db is None
        if db is None:
            return jsonify([]), 200
        
        # Get all centers
        centers = list(db.centers.find({}))
        
        # Convert ObjectId to string for JSON serialization
        for center in centers:
            center['_id'] = str(center['_id'])
        
        return jsonify(centers), 200
    except Exception as e:
        print(f"Error in /api/centers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/filter-options')
@login_required
def api_filter_options():
    """Get filter dropdown options"""
    try:
        db = get_db()
        
        # FIXED: Check if db is None
        if db is None:
            return jsonify({
                "zones": [{"id": "zone_siliguri", "name": "Siliguri Zone"}],
                "regions": [],
                "divisions": []
            }), 200
        
        # Get distinct regions and divisions from centers
        centers = list(db.centers.find({}))
        
        regions = sorted(list(set(c.get('region') for c in centers if c.get('region'))))
        divisions = sorted(list(set(c.get('division') for c in centers if c.get('division'))))
        
        zones = [{
            "id": "zone_siliguri",
            "name": "Siliguri Zone"
        }]
        
        return jsonify({
            "zones": zones,
            "regions": [{"id": r, "name": r} for r in regions],
            "divisions": [{"id": d, "name": d} for d in divisions]
        }), 200
    except Exception as e:
        print(f"Error in /api/filter-options: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/office-details')
@login_required
def api_office_details():
    """Get detailed information for a specific office"""
    try:
        office_type = request.args.get('type')
        office_id = request.args.get('id')
        
        db = get_db()
        
        # FIXED: Check if db is None
        if db is None:
            return jsonify({"error": "Database not connected"}), 500
        
        if office_type == 'zone':
            # Get all centers for zone totals
            centers = list(db.centers.find({}))
            return jsonify({
                "name": "Siliguri Zone",
                "type": "zone",
                "total_centers": len(centers),
                "total_consumers": sum(c.get('total_consumers', 0) for c in centers),
                "total_staff": sum(c.get('total_staff', 0) for c in centers),
                "total_dtr": sum(c.get('total_dtr', 0) for c in centers),
                "centers": centers
            }), 200
            
        elif office_type == 'region':
            # Get all centers in this region
            centers = list(db.centers.find({"region": office_id}))
            return jsonify({
                "name": office_id,
                "type": "region",
                "total_centers": len(centers),
                "total_consumers": sum(c.get('total_consumers', 0) for c in centers),
                "total_staff": sum(c.get('total_staff', 0) for c in centers),
                "total_dtr": sum(c.get('total_dtr', 0) for c in centers),
                "centers": centers
            }), 200
            
        elif office_type == 'division':
            # Get all centers in this division
            centers = list(db.centers.find({"division": office_id}))
            return jsonify({
                "name": office_id,
                "type": "division",
                "total_centers": len(centers),
                "total_consumers": sum(c.get('total_consumers', 0) for c in centers),
                "total_staff": sum(c.get('total_staff', 0) for c in centers),
                "total_dtr": sum(c.get('total_dtr', 0) for c in centers),
                "centers": centers
            }), 200
            
        elif office_type == 'center':
            # Get specific center
            center = db.centers.find_one({"name": office_id})
            if center:
                center['_id'] = str(center['_id'])
                return jsonify(center), 200
            else:
                return jsonify({"error": "Center not found"}), 404
        else:
            return jsonify({"error": "Invalid office type"}), 400
            
    except Exception as e:
        print(f"Error in /api/office-details: {e}")
        return jsonify({"error": str(e)}), 500

# ==================== API ROUTES FOR ADMIN DASHBOARD (Enhanced with MongoDB) ====================

@app.route('/api/admin/data', methods=['POST'])
@login_required
def api_get_admin_data():
    """Get administrative data based on filters from MongoDB"""
    try:
        filters = request.json or {}
        db = get_db()
        
        # FIXED: Check if db is None
        if db is None:
            return jsonify({
                'kpi': {
                    'regions': 0,
                    'divisions': 0,
                    'substations': 0,
                    'staff': 0,
                    'consumers': 0
                },
                'regions': [],
                'divisions': []
            }), 200
        
        # Get all centers
        centers = list(db.centers.find({}))
        
        # Calculate KPI totals
        total_consumers = sum(c.get('total_consumers', 0) for c in centers)
        total_staff = sum(c.get('total_staff', 0) for c in centers)
        total_dtr = sum(c.get('total_dtr', 0) for c in centers)
        
        # Get unique regions and divisions
        unique_regions = len(set(c.get('region') for c in centers if c.get('region')))
        unique_divisions = len(set(c.get('division') for c in centers if c.get('division')))
        
        # Group by region for region cards
        region_dict = {}
        for center in centers:
            region = center.get('region')
            if not region:
                continue
                
            if region not in region_dict:
                region_dict[region] = {
                    'name': region,
                    'divisions': set(),
                    'centers': 0,
                    'consumers': 0,
                    'staff': 0,
                    'dtr': 0
                }
            
            region_dict[region]['divisions'].add(center.get('division'))
            region_dict[region]['centers'] += 1
            region_dict[region]['consumers'] += center.get('total_consumers', 0)
            region_dict[region]['staff'] += center.get('total_staff', 0)
            region_dict[region]['dtr'] += center.get('total_dtr', 0)
        
        # Format regions for response
        regions_data = []
        for region_name, data in region_dict.items():
            regions_data.append({
                'name': region_name,
                'divisions': len(data['divisions']),
                'substations': data['dtr'],  # Using DTR count as substation proxy
                'staff': data['staff'],
                'incharge': 'Regional Manager',  # This would come from regions collection
                'contact': 'N/A'
            })
        
        # Format divisions for table
        divisions_data = []
        sl = 1
        for center in centers:
            divisions_data.append({
                'sl': sl,
                'name': center.get('division', 'N/A'),
                'region': center.get('region', 'N/A'),
                'substations': center.get('total_dtr', 0),
                'aed': center.get('incharge', 'N/A'),
                'contact': center.get('incharge_contact', 'N/A'),
                'email': center.get('incharge_email', 'N/A'),
                'staff': center.get('total_staff', 0),
                'tech': center.get('total_staff', 0)  # Placeholder
            })
            sl += 1
        
        data = {
            'kpi': {
                'regions': unique_regions,
                'divisions': unique_divisions,
                'substations': total_dtr,
                'staff': total_staff,
                'consumers': total_consumers
            },
            'regions': regions_data,
            'divisions': divisions_data
        }
        
        return jsonify(data), 200
    except Exception as e:
        print(f"Error in /api/admin/data: {e}")
        return jsonify({'error': str(e)}), 500

# ==================== API ROUTES FOR TECHNICAL DASHBOARD ====================

@app.route('/api/technical/data', methods=['POST'])
@login_required
def api_get_technical_data():
    """Get technical data based on filters"""
    try:
        filters = request.json or {}
        
        # Sample technical data
        data = {
            'kpi': {
                'ptr_units': 48,
                'ptr_capacity': 520,
                'dtr_units': 845,
                'dtr_capacity': 42.5,
                'line_33kv': 78.5,
                'towers': 245,
                'feeders_11kv': 86,
                'feeder_length': 486
            },
            'ptr': {
                'total': 48,
                'capacity': 520,
                'avg_loading': 68,
                'active': 42,
                'augmentation': 6
            },
            'dtr': {
                'total': 845,
                'capacity': 42.5,
                'failure_rate': 3.2,
                'healthy': 789,
                'failed_mtd': 12
            },
            'line_33kv': {
                'total_length': 78.5,
                'towers': 245,
                'augmentation_progress': 45,
                'zebra': 42,
                'panther': 36.5
            },
            'feeder_11kv': {
                'total': 86,
                'total_length': 486,
                'coverage': 94,
                'operational': 78,
                'maintenance': 8
            },
            'regions': [
                {'region': 'Darjeeling', 'ptr': 18, 'ptr_cap': 195, 'dtr': 312, 
                 'line_33kv': 28.5, 'feeder_11kv': 156, 'peak_load': 6.8},
                {'region': 'Jalpaiguri', 'ptr': 12, 'ptr_cap': 130, 'dtr': 198, 
                 'line_33kv': 22.0, 'feeder_11kv': 128, 'peak_load': 8.2},
                {'region': 'Coochbehar', 'ptr': 10, 'ptr_cap': 108, 'dtr': 156, 
                 'line_33kv': 15.5, 'feeder_11kv': 112, 'peak_load': 4.5},
                {'region': 'Alipurduar', 'ptr': 8, 'ptr_cap': 87, 'dtr': 179, 
                 'line_33kv': 12.5, 'feeder_11kv': 90, 'peak_load': 3.2}
            ]
        }
        
        return jsonify(data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== API ROUTES FOR COMMERCIAL DASHBOARD ====================

@app.route('/api/commercial/data', methods=['POST'])
@login_required
def api_get_commercial_data():
    """Get commercial data based on filters"""
    try:
        filters = request.json or {}
        
        # Get period from filters
        period = filters.get('period', 'current_month')
        compare = filters.get('compare', 'none')
        parameter = filters.get('parameter', 'input')
        
        # Sample commercial data
        data = {
            'kpi': {
                'input': 185.6,
                'demand': 168.2,
                'collection': 42.5,
                'collection_eff': 94.2
            },
            'divisions': [
                {'sl': 1, 'name': 'Balurghat Div Total', 'jan': 27.66, 'feb': 26.84, 'mar': '-', 
                 'target': 25.22, 'status': 'above', 'yoy_growth': 8.2},
                {'sl': 2, 'name': 'Buniadpur Div Total', 'jan': 31.31, 'feb': 30.45, 'mar': '-', 
                 'target': 28.07, 'status': 'above', 'yoy_growth': 6.5},
                {'sl': 3, 'name': 'Chanchal Div Total', 'jan': 32.89, 'feb': 31.92, 'mar': '-', 
                 'target': 31.07, 'status': 'above', 'yoy_growth': 5.8},
                {'sl': 4, 'name': 'Gazole Div Total', 'jan': 28.56, 'feb': 27.71, 'mar': '-', 
                 'target': 26.82, 'status': 'above', 'yoy_growth': 7.1},
                {'sl': 5, 'name': 'Islampur Div Total', 'jan': 35.48, 'feb': 34.42, 'mar': '-', 
                 'target': 31.46, 'status': 'above', 'yoy_growth': 9.3},
                {'sl': 6, 'name': 'Malda Div Total', 'jan': 19.69, 'feb': 20.15, 'mar': '-', 
                 'target': 22.08, 'status': 'below', 'yoy_growth': -2.4},
                {'sl': 7, 'name': 'North Malda Div Total', 'jan': '-', 'feb': '-', 'mar': '-', 
                 'target': '-', 'status': 'na', 'yoy_growth': 0},
                {'sl': 8, 'name': 'Raiganj Div Total', 'jan': 24.58, 'feb': 23.89, 'mar': '-', 
                 'target': 23.91, 'status': 'above', 'yoy_growth': 4.2},
                {'sl': 9, 'name': 'South Malda Div Total', 'jan': '-', 'feb': '-', 'mar': '-', 
                 'target': '-', 'status': 'na', 'yoy_growth': 0}
            ],
            'summary': {
                'atc_loss': 18.5,
                'atc_loss_change': 2.1,
                'td_loss': 12.3,
                'td_loss_change': -1.2,
                'collection_eff': 94.2,
                'collection_eff_change': 3.1,
                'outstanding': 8.2,
                'outstanding_change': 0.5
            },
            'monthly_trend': {
                'months': ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar'],
                'input': [152.3, 158.7, 162.4, 168.9, 172.3, 175.6, 178.2, 180.5, 182.1, 185.6, 187.2, 189.5],
                'demand': [142.1, 148.3, 152.6, 158.2, 161.5, 164.8, 167.3, 169.5, 171.2, 168.2, 170.5, 172.8],
                'collection': [36.2, 37.8, 38.9, 40.2, 41.1, 41.8, 42.3, 42.8, 42.9, 42.5, 43.1, 43.5]
            }
        }
        
        # Apply region filter if provided
        if filters.get('region') and filters['region'] != 'all':
            # In real implementation, filter divisions by region
            pass
            
        if filters.get('division') and filters['division'] != 'all':
            # Filter by specific division
            div_filter = filters['division']
            data['divisions'] = [d for d in data['divisions'] 
                                 if d['name'].lower().replace(' ', '-') == div_filter.lower()]
        
        return jsonify(data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== EXPORT API ROUTES ====================

@app.route('/api/export/<dashboard_type>/<format>', methods=['POST'])
@login_required
def api_export_data(dashboard_type, format):
    """Export dashboard data in specified format (pdf, excel, csv)"""
    try:
        filters = request.json or {}
        
        # Get data based on dashboard type
        if dashboard_type == 'admin':
            response = requests.post(url_for('api_get_admin_data', _external=True), json=filters)
            data = response.json() if response.status_code == 200 else {}
        elif dashboard_type == 'technical':
            response = requests.post(url_for('api_get_technical_data', _external=True), json=filters)
            data = response.json() if response.status_code == 200 else {}
        elif dashboard_type == 'commercial':
            response = requests.post(url_for('api_get_commercial_data', _external=True), json=filters)
            data = response.json() if response.status_code == 200 else {}
        else:
            return jsonify({'error': 'Invalid dashboard type'}), 400
        
        if format == 'csv':
            return export_as_csv(dashboard_type, data, filters)
        elif format == 'excel':
            return export_as_excel(dashboard_type, data, filters)
        elif format == 'pdf':
            return export_as_pdf(dashboard_type, data, filters)
        else:
            return jsonify({'error': 'Invalid export format'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def export_as_csv(dashboard_type, data, filters):
    """Export data as CSV file"""
    try:
        output = StringIO()
        writer = csv.writer(output)
        
        # Add header with filter information
        writer.writerow([f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
        writer.writerow([f"Dashboard: {dashboard_type.title()}"])
        writer.writerow([f"Filters: {json.dumps(filters)}"])
        writer.writerow([])
        
        if dashboard_type == 'admin' and 'divisions' in data:
            # Write admin data
            writer.writerow(['Sl.', 'Division', 'Region', 'Substations', 'AED Incharge', 'Contact', 'Email', 'Staff', 'Tech Staff'])
            for div in data['divisions']:
                writer.writerow([
                    div.get('sl', ''),
                    div.get('name', ''),
                    div.get('region', ''),
                    div.get('substations', ''),
                    div.get('aed', ''),
                    div.get('contact', ''),
                    div.get('email', ''),
                    div.get('staff', ''),
                    div.get('tech', '')
                ])
        
        elif dashboard_type == 'commercial' and 'divisions' in data:
            # Write commercial data
            writer.writerow(['Sl.', 'Unit', 'Jan 2026', 'Feb 2026', 'Mar 2026', 'Change %', 'Target', 'vs Target', 'Status'])
            for div in data['divisions']:
                change = '-'
                if div.get('jan') != '-' and div.get('feb') != '-':
                    change = f"{((float(div['jan']) - float(div['feb'])) / float(div['feb']) * 100):.1f}%"
                
                vs_target = '-'
                if div.get('jan') != '-' and div.get('target') != '-':
                    vs_target = f"{float(div['jan']) - float(div['target']):.2f}"
                
                writer.writerow([
                    div.get('sl', ''),
                    div.get('name', ''),
                    div.get('jan', ''),
                    div.get('feb', ''),
                    div.get('mar', ''),
                    change,
                    div.get('target', ''),
                    vs_target,
                    div.get('status', '').title()
                ])
        
        # Create response
        output.seek(0)
        filename = f"{dashboard_type}_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        response.headers['Content-type'] = 'text/csv'
        
        return response
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def export_as_excel(dashboard_type, data, filters):
    """Export data as Excel file"""
    try:
        # Create Excel file using pandas
        with BytesIO() as output:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                if dashboard_type == 'admin' and 'divisions' in data:
                    # Create admin dataframe
                    df = pd.DataFrame(data['divisions'])
                    df.to_excel(writer, sheet_name='Admin Data', index=False)
                    
                    # Add summary sheet
                    if 'kpi' in data:
                        kpi_df = pd.DataFrame([data['kpi']])
                        kpi_df.to_excel(writer, sheet_name='Summary', index=False)
                
                elif dashboard_type == 'commercial' and 'divisions' in data:
                    # Create commercial dataframe
                    df = pd.DataFrame(data['divisions'])
                    df.to_excel(writer, sheet_name='Commercial Data', index=False)
                    
                    # Add summary sheet
                    if 'summary' in data:
                        summary_df = pd.DataFrame([data['summary']])
                        summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Add trend sheet
                    if 'monthly_trend' in data:
                        trend_df = pd.DataFrame(data['monthly_trend'])
                        trend_df.to_excel(writer, sheet_name='Monthly Trend', index=False)
                
                elif dashboard_type == 'technical' and 'regions' in data:
                    # Create technical dataframe
                    df = pd.DataFrame(data['regions'])
                    df.to_excel(writer, sheet_name='Technical Data', index=False)
                    
                    # Add asset sheets
                    if 'ptr' in data:
                        ptr_df = pd.DataFrame([data['ptr']])
                        ptr_df.to_excel(writer, sheet_name='PTR Summary', index=False)
                    
                    if 'dtr' in data:
                        dtr_df = pd.DataFrame([data['dtr']])
                        dtr_df.to_excel(writer, sheet_name='DTR Summary', index=False)
            
            # Create response
            output.seek(0)
            filename = f"{dashboard_type}_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            response = make_response(output.getvalue())
            response.headers['Content-Disposition'] = f'attachment; filename={filename}'
            response.headers['Content-type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            
            return response
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def export_as_pdf(dashboard_type, data, filters):
    """Export data as PDF file"""
    # For PDF generation, you would need a library like WeasyPrint or ReportLab
    # This is a placeholder that returns a message
    return jsonify({
        'message': 'PDF export functionality will be implemented with a PDF library',
        'dashboard_type': dashboard_type,
        'filters': filters
    }), 200

# ==================== REGION API ROUTES (Existing) ====================

@app.route('/api/region/<region_id>')
@login_required
def api_get_region(region_id):
    """Get region details and its divisions"""
    try:
        region = FilterDB.get_region_by_id(region_id)
        divisions = FilterDB.get_divisions_by_region(region_id)
        
        return jsonify({
            'region': region,
            'divisions': divisions
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/division/<division_id>')
@login_required
def api_get_division(division_id):
    """Get division details and its substations"""
    try:
        division = FilterDB.get_division_by_id(division_id)
        substations = FilterDB.get_substations_by_division(division_id)
        
        return jsonify({
            'division': division,
            'substations': substations
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/substation/<substation_id>')
@login_required
def api_get_substation(substation_id):
    """Get substation details and its components"""
    try:
        details = FilterDB.get_substation_details(substation_id)
        ptrs = FilterDB.get_ptrs_by_substation(substation_id)
        lines_33kv = FilterDB.get_33kv_lines_by_substation(substation_id)
        feeders_11kv = FilterDB.get_11kv_feeders_by_substation(substation_id)
        
        return jsonify({
            'substation': details,
            'ptrs': ptrs,
            'lines_33kv': lines_33kv,
            'feeders_11kv': feeders_11kv
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== EXISTING DASHBOARD ROUTES ====================

@app.route('/substation-dashboard')
@login_required
def substation_dashboard():
    """33/11 KV Sub-Station main dashboard"""
    user = session['user']
    return render_template('dashboards/substation_33_11kv_dashboard.html', user=user)

@app.route('/new-substation-dashboard')
@login_required
def new_substation_dashboard():
    """New 33/11 KV Sub-Station dashboard"""
    user = session['user']
    return render_template('dashboards/New_33_11kv_Sub-Stn_dashboard.html', user=user)

@app.route('/ptr-augmentation-dashboard')
@login_required
def ptr_augmentation_dashboard():
    """PTR Augmentation dashboard"""
    user = session['user']
    return render_template('dashboards/PTR_Augmentation_dashboard.html', user=user)

@app.route('/hvds-dashboard')
@login_required
def hvds_dashboard():
    """HVDS main dashboard"""
    user = session['user']
    return render_template('dashboards/hvds_dashboard.html', user=user)

@app.route('/line-33kv-dashboard')
@login_required
def line_33kv_dashboard():
    """33 KV Line main dashboard"""
    user = session['user']
    return render_template('dashboards/line_33kv_dashboard.html', user=user)

@app.route('/new-33kv-line-dashboard')
@login_required
def new_33kv_line_dashboard():
    """New 33KV Line dashboard"""
    user = session['user']
    return render_template('dashboards/New_33KV_Line_dashboard.html', user=user)

@app.route('/conductor-augmt-33kv-dashboard')
@login_required
def conductor_augmt_33kv_dashboard():
    """33KV Conductor Augmentation dashboard"""
    user = session['user']
    return render_template('dashboards/Conductor_Augmt_33KV_Line_dashboard.html', user=user)

@app.route('/line-11kv-dashboard')
@login_required
def line_11kv_dashboard():
    """11 KV Line main dashboard"""
    user = session['user']
    return render_template('dashboards/line_11kv_dashboard.html', user=user)

@app.route('/new-11kv-line-dashboard')
@login_required
def new_11kv_line_dashboard():
    """New 11KV Line dashboard"""
    user = session['user']
    return render_template('dashboards/New_11KV_11_Line_dashboard.html', user=user)

@app.route('/conductor-augmt-11kv-dashboard')
@login_required
def conductor_augmt_11kv_dashboard():
    """11KV Conductor Augmentation dashboard"""
    user = session['user']
    return render_template('dashboards/Conductor_Augmt_11_Line_dashboard.html', user=user)

# ==================== TEST ROUTE ====================

@app.route('/my-role')
@login_required
def my_role():
    """Simple page to check user role"""
    user = session['user']
    return f"""
    <h2>Your Information</h2>
    <p>Email: {user['email']}</p>
    <p>Role: {user['role']}</p>
    <p>User ID: {user['id']}</p>
    <br>
    <a href="/general-overview">Back to General Overview</a>
    """

# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def page_not_found(e):
    flash('Page not found', 'warning')
    if 'user' in session:
        return redirect(url_for('general_overview'))
    return redirect(url_for('login'))

@app.errorhandler(500)
def internal_server_error(e):
    flash('Server error. Please try again.', 'danger')
    if 'user' in session:
        return redirect(url_for('general_overview'))
    return redirect(url_for('login'))

# ==================== HEALTH CHECK ====================

@app.route('/healthz')
def healthz():
    return "OK", 200

# ==================== RUN APPLICATION ====================

if __name__ == '__main__':
    # Initialize master data if needed
    init_master_data()
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)