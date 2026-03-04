"""
INVENTORY MANAGEMENT SYSTEM - SILIGURI ZONE
Main Application File with Database Integration and Advanced Filtering
Enhanced Version with Complete Filter Support and Export Functionality
"""

import os
import csv
import io
import json
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, Response, send_file
from supabase import create_client
from dotenv import load_dotenv
from datetime import timedelta, datetime
from functools import wraps

# Try to import pandas, but make it optional
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Warning: pandas not installed. Excel export will be disabled. Install with: pip install pandas openpyxl")

# Import our database helper
from supabase_utils import ProjectDB, ReferenceDB, FilterDB

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'inventory-management-siliguri-secret-key-2026')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)
app.config['JSON_SORT_KEYS'] = False  # Preserve JSON key order

# Get Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_ANON_KEY')

# Initialize Supabase for auth (DB operations go through our helper)
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

def get_current_user():
    """Get current user from session"""
    return session.get('user')

def format_response(data, status=200):
    """Format JSON response consistently"""
    return app.response_class(
        response=json.dumps(data, default=str, indent=2),
        status=status,
        mimetype='application/json'
    )

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
                # Get user's role and division from profiles table
                profile = supabase.table('profiles')\
                    .select('role, region, division, section, full_name')\
                    .eq('id', auth_response.user.id)\
                    .execute()
                
                role = 'user'
                region = None
                division = None
                section = None
                full_name = None
                
                if profile.data and len(profile.data) > 0:
                    role = profile.data[0].get('role', 'user')
                    region = profile.data[0].get('region')
                    division = profile.data[0].get('division')
                    section = profile.data[0].get('section')
                    full_name = profile.data[0].get('full_name')
                
                # Store user info in session
                session['user'] = {
                    'id': auth_response.user.id,
                    'email': auth_response.user.email,
                    'full_name': full_name,
                    'role': role,
                    'region': region,
                    'division': division,
                    'section': section
                }
                session.permanent = True
                
                flash(f'Welcome back, {full_name or email}!', 'success')
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

# ==================== SUBSTATION DASHBOARD ROUTES ====================

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

# ==================== HVDS DASHBOARD ROUTES ====================

@app.route('/hvds-dashboard')
@login_required
def hvds_dashboard():
    """HVDS main dashboard"""
    user = session['user']
    return render_template('dashboards/hvds_dashboard.html', user=user)

# ==================== 33KV LINE DASHBOARD ROUTES ====================

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

# ==================== 11KV LINE DASHBOARD ROUTES ====================

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

# ==================== SILIGURI ZONE FILTER API ENDPOINTS ====================

@app.route('/api/siliguri/regions')
@login_required
def api_get_siliguri_regions():
    """Get all regions in Siliguri Zone"""
    try:
        regions = [
            {"id": "darjeeling", "name": "Darjeeling", "count": 5},
            {"id": "jalpaiguri", "name": "Jalpaiguri", "count": 2},
            {"id": "coochbehar", "name": "Coochbehar", "count": 3},
            {"id": "alipurduar", "name": "Alipurduar", "count": 1}
        ]
        return jsonify({'regions': regions}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/siliguri/divisions/<region>')
@login_required
def api_get_siliguri_divisions(region):
    """Get divisions for a specific region in Siliguri Zone"""
    try:
        divisions_map = {
            'darjeeling': [
                {"id": "siliguri-town", "name": "Siliguri Town Division", "substations": 4, "feeders": 12},
                {"id": "kurseong", "name": "Kurseong Division", "substations": 3, "feeders": 8},
                {"id": "darjeeling", "name": "Darjeeling Division", "substations": 2, "feeders": 5},
                {"id": "sub-urban", "name": "Sub-Urban Division", "substations": 1, "feeders": 3},
                {"id": "kalimpong", "name": "Kalimpong Division", "substations": 1, "feeders": 2}
            ],
            'jalpaiguri': [
                {"id": "jalpaiguri", "name": "Jalpaiguri Division", "substations": 4, "feeders": 15},
                {"id": "mal", "name": "Mal Division", "substations": 2, "feeders": 9}
            ],
            'coochbehar': [
                {"id": "coochbehar", "name": "Coochbehar Division", "substations": 2, "feeders": 7},
                {"id": "mathabhanga", "name": "Mathabhanga Division", "substations": 2, "feeders": 6},
                {"id": "dinhata", "name": "Dinhata Division", "substations": 1, "feeders": 5}
            ],
            'alipurduar': [
                {"id": "alipurduar", "name": "Alipurduar Division", "substations": 3, "feeders": 16}
            ]
        }
        
        divisions = divisions_map.get(region.lower(), [])
        return jsonify({'divisions': divisions}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/siliguri/substations/<division>')
@login_required
def api_get_siliguri_substations(division):
    """Get substations for a specific division in Siliguri Zone"""
    try:
        substations_map = {
            'siliguri-town': [
                {"id": "city-center", "name": "City Center 33/11 KV", "feeders": 4, "capacity": "40 MVA"},
                {"id": "industrial-area", "name": "Industrial Area 33/11 KV", "feeders": 3, "capacity": "30 MVA"},
                {"id": "matigara", "name": "Matigara 33/11 KV", "feeders": 3, "capacity": "25 MVA"},
                {"id": "bagdogra", "name": "Bagdogra 33/11 KV", "feeders": 2, "capacity": "20 MVA"}
            ],
            'kurseong': [
                {"id": "kurseong-town", "name": "Kurseong Town 33/11 KV", "feeders": 3, "capacity": "20 MVA"},
                {"id": "sonada", "name": "Sonada 33/11 KV", "feeders": 3, "capacity": "15 MVA"},
                {"id": "sukna", "name": "Sukna 33/11 KV", "feeders": 2, "capacity": "10 MVA"}
            ]
        }
        
        substations = substations_map.get(division.lower(), [])
        return jsonify({'substations': substations}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== 33KV LINE FILTER API ====================

@app.route('/api/33kv/lines/filtered', methods=['GET'])
@login_required
def api_get_filtered_33kv_lines():
    """Get filtered 33KV lines with pagination"""
    try:
        user = get_current_user()
        
        # Build filters
        filters = {}
        
        # Region filter
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        
        # Division filter
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        
        # Substation filter
        if request.args.get('substation') and request.args.get('substation') != 'all':
            filters['substation'] = request.args.get('substation')
        
        # Status filter
        if request.args.get('status') and request.args.get('status') != 'all':
            filters['status'] = request.args.get('status')
        
        # Conductor type filter
        if request.args.get('conductor') and request.args.get('conductor') != 'all':
            filters['conductor'] = request.args.get('conductor')
        
        # Load range filters
        if request.args.get('load_min'):
            filters['load_min'] = float(request.args.get('load_min'))
        if request.args.get('load_max'):
            filters['load_max'] = float(request.args.get('load_max'))
        
        # Length range filters
        if request.args.get('length_min'):
            filters['length_min'] = float(request.args.get('length_min'))
        if request.args.get('length_max'):
            filters['length_max'] = float(request.args.get('length_max'))
        
        # Top N filter
        top_n = request.args.get('top', 'all')
        
        # Get filtered lines from database
        lines = FilterDB.get_filtered_33kv_lines(user['id'], filters, top_n)
        
        # Apply sorting
        sort_by = request.args.get('sort_by', 'load')
        sort_order = request.args.get('sort_order', 'desc')
        
        if sort_by == 'load':
            lines = sorted(lines, key=lambda x: x.get('load_mva', 0), reverse=(sort_order == 'desc'))
        elif sort_by == 'length':
            lines = sorted(lines, key=lambda x: x.get('length_km', 0), reverse=(sort_order == 'desc'))
        elif sort_by == 'name':
            lines = sorted(lines, key=lambda x: x.get('name', ''), reverse=(sort_order == 'desc'))
        
        return jsonify({
            'lines': lines,
            'count': len(lines),
            'filters': filters
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/33kv/stats')
@login_required
def api_get_33kv_stats():
    """Get comprehensive statistics for 33KV network"""
    try:
        user = get_current_user()
        
        # Build filters
        filters = {}
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        
        stats = FilterDB.get_33kv_network_stats(user['id'], filters)
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/33kv/region-breakdown')
@login_required
def api_get_33kv_region_breakdown():
    """Get region-wise breakdown of 33KV network"""
    try:
        user = get_current_user()
        breakdown = FilterDB.get_33kv_region_breakdown(user['id'])
        return jsonify({'breakdown': breakdown}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== 11KV LINE FILTER API ====================

@app.route('/api/11kv/lines/filtered', methods=['GET'])
@login_required
def api_get_filtered_11kv_lines():
    """Get filtered 11KV lines/feeders with pagination"""
    try:
        user = get_current_user()
        
        # Build filters
        filters = {}
        
        # Region filter
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        
        # Division filter
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        
        # Substation filter
        if request.args.get('substation') and request.args.get('substation') != 'all':
            filters['substation'] = request.args.get('substation')
        
        # Status filter
        if request.args.get('status') and request.args.get('status') != 'all':
            filters['status'] = request.args.get('status')
        
        # Load range filters
        if request.args.get('load_min'):
            filters['load_min'] = float(request.args.get('load_min'))
        if request.args.get('load_max'):
            filters['load_max'] = float(request.args.get('load_max'))
        
        # Length range filters
        if request.args.get('length_min'):
            filters['length_min'] = float(request.args.get('length_min'))
        if request.args.get('length_max'):
            filters['length_max'] = float(request.args.get('length_max'))
        
        # Top N filter
        top_n = request.args.get('top', 'all')
        
        # Get filtered feeders from database
        feeders = FilterDB.get_filtered_11kv_feeders(user['id'], filters, top_n)
        
        # Apply sorting
        sort_by = request.args.get('sort_by', 'load')
        sort_order = request.args.get('sort_order', 'desc')
        
        if sort_by == 'load':
            feeders = sorted(feeders, key=lambda x: x.get('load_mva', 0), reverse=(sort_order == 'desc'))
        elif sort_by == 'length':
            feeders = sorted(feeders, key=lambda x: x.get('length_km', 0), reverse=(sort_order == 'desc'))
        elif sort_by == 'dtrs':
            feeders = sorted(feeders, key=lambda x: x.get('dtr_count', 0), reverse=(sort_order == 'desc'))
        elif sort_by == 'name':
            feeders = sorted(feeders, key=lambda x: x.get('name', ''), reverse=(sort_order == 'desc'))
        
        return jsonify({
            'feeders': feeders,
            'count': len(feeders),
            'filters': filters
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/11kv/stats')
@login_required
def api_get_11kv_stats():
    """Get comprehensive statistics for 11KV network"""
    try:
        user = get_current_user()
        
        # Build filters
        filters = {}
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        
        stats = FilterDB.get_11kv_network_stats(user['id'], filters)
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/11kv/region-breakdown')
@login_required
def api_get_11kv_region_breakdown():
    """Get region-wise breakdown of 11KV network"""
    try:
        user = get_current_user()
        breakdown = FilterDB.get_11kv_region_breakdown(user['id'])
        return jsonify({'breakdown': breakdown}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/11kv/top-feeders')
@login_required
def api_get_top_11kv_feeders():
    """Get top 10 11KV feeders by load"""
    try:
        user = get_current_user()
        limit = request.args.get('limit', 10, type=int)
        top_feeders = FilterDB.get_top_11kv_feeders(user['id'], limit)
        return jsonify({'top_feeders': top_feeders}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== EXPORT API FOR 11KV ====================

@app.route('/api/11kv/export/<format>', methods=['GET'])
@login_required
def api_export_11kv_data(format):
    """Export 11KV feeder data in various formats"""
    try:
        user = get_current_user()
        
        # Build filters
        filters = {}
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        if request.args.get('substation') and request.args.get('substation') != 'all':
            filters['substation'] = request.args.get('substation')
        if request.args.get('status') and request.args.get('status') != 'all':
            filters['status'] = request.args.get('status')
        
        # Get export data
        top_n = request.args.get('top', 'all')
        export_data = FilterDB.get_11kv_export_data(user['id'], filters, top_n)
        
        if not export_data:
            return jsonify({'error': 'No data to export'}), 404
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"siliguri_11kv_feeders_{timestamp}"
        
        if format == 'csv':
            # Create CSV in memory
            output = io.StringIO()
            if export_data:
                writer = csv.DictWriter(output, fieldnames=export_data[0].keys())
                writer.writeheader()
                writer.writerows(export_data)
            
            # Create response
            response = Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={
                    'Content-Disposition': f'attachment; filename={filename}.csv'
                }
            )
            return response
            
        elif format == 'excel':
            # Check if pandas is available
            if not PANDAS_AVAILABLE:
                return jsonify({'error': 'Excel export requires pandas. Please install: pip install pandas openpyxl'}), 400
            
            # Create Excel file in memory
            output = io.BytesIO()
            df = pd.DataFrame(export_data)
            
            # Write to Excel
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='11KV Feeders', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['11KV Feeders']
                for column in df:
                    column_width = max(df[column].astype(str).map(len).max(), len(column))
                    col_idx = df.columns.get_loc(column)
                    worksheet.column_dimensions[chr(65 + col_idx)].width = min(column_width + 2, 30)
            
            output.seek(0)
            
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f'{filename}.xlsx'
            )
            
        elif format == 'json':
            return jsonify({
                'data': export_data,
                'count': len(export_data),
                'filters': filters,
                'exported_at': datetime.now().isoformat()
            })
        
        else:
            return jsonify({'error': 'Unsupported format'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== EXPORT API FOR 33KV ====================

@app.route('/api/33kv/export/<format>', methods=['GET'])
@login_required
def api_export_33kv_data(format):
    """Export 33KV line data in various formats"""
    try:
        user = get_current_user()
        
        # Build filters
        filters = {}
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        if request.args.get('substation') and request.args.get('substation') != 'all':
            filters['substation'] = request.args.get('substation')
        if request.args.get('status') and request.args.get('status') != 'all':
            filters['status'] = request.args.get('status')
        
        # Get export data
        top_n = request.args.get('top', 'all')
        export_data = FilterDB.get_33kv_export_data(user['id'], filters, top_n)
        
        if not export_data:
            return jsonify({'error': 'No data to export'}), 404
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"siliguri_33kv_lines_{timestamp}"
        
        if format == 'csv':
            # Create CSV in memory
            output = io.StringIO()
            if export_data:
                writer = csv.DictWriter(output, fieldnames=export_data[0].keys())
                writer.writeheader()
                writer.writerows(export_data)
            
            # Create response
            response = Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={
                    'Content-Disposition': f'attachment; filename={filename}.csv'
                }
            )
            return response
            
        elif format == 'excel':
            # Check if pandas is available
            if not PANDAS_AVAILABLE:
                return jsonify({'error': 'Excel export requires pandas. Please install: pip install pandas openpyxl'}), 400
            
            # Create Excel file in memory
            output = io.BytesIO()
            df = pd.DataFrame(export_data)
            
            # Write to Excel
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='33KV Lines', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['33KV Lines']
                for column in df:
                    column_width = max(df[column].astype(str).map(len).max(), len(column))
                    col_idx = df.columns.get_loc(column)
                    worksheet.column_dimensions[chr(65 + col_idx)].width = min(column_width + 2, 30)
            
            output.seek(0)
            
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f'{filename}.xlsx'
            )
            
        elif format == 'json':
            return jsonify({
                'data': export_data,
                'count': len(export_data),
                'filters': filters,
                'exported_at': datetime.now().isoformat()
            })
        
        else:
            return jsonify({'error': 'Unsupported format'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== ADVANCED FILTERING API ====================

@app.route('/api/projects/filtered', methods=['GET'])
@login_required
def api_get_filtered_projects():
    """Get projects with advanced filtering and pagination"""
    try:
        user = get_current_user()
        
        # Build filters from request args
        filters = {}
        
        # Basic filters
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        if request.args.get('status') and request.args.get('status') != 'all':
            filters['status'] = request.args.get('status')
        if request.args.get('priority') and request.args.get('priority') != 'all':
            filters['priority'] = request.args.get('priority')
        if request.args.get('project_type') and request.args.get('project_type') != 'all':
            filters['project_type'] = request.args.get('project_type')
        
        # Progress range filters
        if request.args.get('progress_min'):
            filters['progress_min'] = int(request.args.get('progress_min'))
        if request.args.get('progress_max'):
            filters['progress_max'] = int(request.args.get('progress_max'))
        
        # Date range filters
        if request.args.get('start_date'):
            filters['start_date'] = request.args.get('start_date')
        if request.args.get('end_date'):
            filters['end_date'] = request.args.get('end_date')
        
        # Search filter
        if request.args.get('search'):
            filters['search'] = request.args.get('search')
        
        # JSONB field filters (for custom fields)
        json_filters = {}
        if request.args.get('budget_min'):
            json_filters['budget'] = {'min': request.args.get('budget_min')}
        if request.args.get('contractor'):
            json_filters['contractor'] = request.args.get('contractor')
        if json_filters:
            filters['json_filters'] = json_filters
        
        # Sorting
        if request.args.get('sort_by'):
            filters['sort_by'] = request.args.get('sort_by')
        if request.args.get('sort_order'):
            filters['sort_order'] = request.args.get('sort_order')
        
        # Pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        
        # Get filtered projects with pagination
        result = ProjectDB.get_filtered_projects_advanced(
            user['id'], filters, page, per_page
        )
        
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== FILTER STATS API ====================

@app.route('/api/stats/filtered', methods=['GET'])
@login_required
def api_get_filtered_stats():
    """Get comprehensive statistics for filtered data"""
    try:
        user = get_current_user()
        
        # Build filters
        filters = {}
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        if request.args.get('status') and request.args.get('status') != 'all':
            filters['status'] = request.args.get('status')
        if request.args.get('priority') and request.args.get('priority') != 'all':
            filters['priority'] = request.args.get('priority')
        
        stats = FilterDB.get_filter_stats(user['id'], filters)
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats/timeline', methods=['GET'])
@login_required
def api_get_timeline_stats():
    """Get timeline-based statistics"""
    try:
        user = get_current_user()
        
        filters = {}
        if request.args.get('region') and request.args.get('region') != 'all':
            filters['region'] = request.args.get('region')
        if request.args.get('division') and request.args.get('division') != 'all':
            filters['division'] = request.args.get('division')
        
        timeline = FilterDB.get_timeline_stats(user['id'], filters)
        return jsonify(timeline), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== API ROUTES (Existing) ====================

@app.route('/api/projects', methods=['GET'])
@login_required
def api_get_projects():
    """Get all projects with optional filters (existing)"""
    try:
        user = get_current_user()
        filters = {}
        
        # Get filter parameters from request
        if request.args.get('project_type'):
            filters['project_type'] = request.args.get('project_type')
        if request.args.get('priority'):
            filters['priority'] = request.args.get('priority')
        if request.args.get('division'):
            filters['division'] = request.args.get('division')
        if request.args.get('status'):
            filters['status'] = request.args.get('status')
        
        projects = ProjectDB.get_projects(user['id'], filters)
        return jsonify({'projects': projects}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<int:project_id>', methods=['GET'])
@login_required
def api_get_project(project_id):
    """Get a single project by ID"""
    try:
        user = get_current_user()
        project = ProjectDB.get_project_by_id(user['id'], project_id)
        
        if not project:
            return jsonify({'error': 'Project not found or access denied'}), 404
            
        return jsonify({'project': project}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects', methods=['POST'])
@login_required
def api_create_project():
    """Create a new project"""
    try:
        data = request.json
        user = get_current_user()
        
        # Add user's division as default if not specified
        if 'division' not in data and user.get('division'):
            data['division'] = user['division']
        
        project = ProjectDB.create_project(user['id'], data)
        
        if project:
            return jsonify({'project': project, 'message': 'Project created successfully'}), 201
        else:
            return jsonify({'error': 'Failed to create project'}), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<int:project_id>', methods=['PUT'])
@login_required
def api_update_project(project_id):
    """Update a project"""
    try:
        data = request.json
        user = get_current_user()
        
        project = ProjectDB.update_project(user['id'], project_id, data)
        
        if project:
            return jsonify({'project': project, 'message': 'Project updated successfully'}), 200
        else:
            return jsonify({'error': 'Failed to update project or access denied'}), 403
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<int:project_id>', methods=['DELETE'])
@login_required
def api_delete_project(project_id):
    """Delete a project (admin/authority only)"""
    try:
        user = get_current_user()
        
        success = ProjectDB.delete_project(user['id'], project_id)
        
        if success:
            return jsonify({'message': 'Project deleted successfully'}), 200
        else:
            return jsonify({'error': 'Failed to delete project or insufficient permissions'}), 403
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== PROJECT UPDATES API ====================

@app.route('/api/projects/<int:project_id>/updates', methods=['GET'])
@login_required
def api_get_updates(project_id):
    """Get all updates for a project"""
    try:
        user = get_current_user()
        updates = ProjectDB.get_updates(user['id'], project_id)
        return jsonify({'updates': updates}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<int:project_id>/updates', methods=['POST'])
@login_required
def api_add_update(project_id):
    """Add an update to a project"""
    try:
        data = request.json
        user = get_current_user()
        
        update = ProjectDB.add_update(
            user['id'],
            project_id,
            data.get('update_text'),
            data.get('progress_before'),
            data.get('progress_after')
        )
        
        if update:
            return jsonify({'update': update, 'message': 'Update added successfully'}), 201
        else:
            return jsonify({'error': 'Failed to add update or access denied'}), 403
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== PROJECT ACTIONS API ====================

@app.route('/api/projects/<int:project_id>/actions', methods=['GET'])
@login_required
def api_get_actions(project_id):
    """Get all actions for a project"""
    try:
        user = get_current_user()
        actions = ProjectDB.get_actions(user['id'], project_id)
        return jsonify({'actions': actions}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<int:project_id>/actions', methods=['POST'])
@login_required
def api_add_action(project_id):
    """Add an action to a project"""
    try:
        data = request.json
        user = get_current_user()
        
        action = ProjectDB.add_action(
            user['id'],
            project_id,
            data.get('action_text'),
            data.get('assigned_to'),
            data.get('due_date')
        )
        
        if action:
            return jsonify({'action': action, 'message': 'Action added successfully'}), 201
        else:
            return jsonify({'error': 'Failed to add action or access denied'}), 403
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/actions/<int:action_id>/complete', methods=['POST'])
@login_required
def api_complete_action(action_id):
    """Mark an action as complete"""
    try:
        user = get_current_user()
        
        action = ProjectDB.complete_action(user['id'], action_id)
        
        if action:
            return jsonify({'action': action, 'message': 'Action completed successfully'}), 200
        else:
            return jsonify({'error': 'Failed to complete action or access denied'}), 403
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== REFERENCE DATA API ====================

@app.route('/api/reference/divisions')
@login_required
def api_get_divisions():
    """Get all divisions"""
    return jsonify({'divisions': ReferenceDB.get_divisions()})

@app.route('/api/reference/regions')
@login_required
def api_get_regions():
    """Get all regions"""
    return jsonify({'regions': ReferenceDB.get_regions()})

@app.route('/api/reference/project-types')
@login_required
def api_get_project_types():
    """Get all project types"""
    return jsonify({'project_types': ReferenceDB.get_project_types()})

@app.route('/api/reference/priorities')
@login_required
def api_get_priorities():
    """Get all priorities"""
    return jsonify({'priorities': ReferenceDB.get_priorities()})

@app.route('/api/reference/statuses')
@login_required
def api_get_reference_statuses():
    """Get all project statuses"""
    return jsonify({'statuses': ReferenceDB.get_statuses()})

@app.route('/api/reference/conductor-types')
@login_required
def api_get_conductor_types():
    """Get all conductor types for 33KV/11KV lines"""
    conductor_types = [
        {"id": "acsr_panther", "name": "ACSR Panther (150 mm²)"},
        {"id": "acsr_zebra", "name": "ACSR Zebra (200 mm²)"},
        {"id": "acsr_dog", "name": "ACSR Dog (100 mm²)"},
        {"id": "acsr_rabbit", "name": "ACSR Rabbit (80 mm²)"},
        {"id": "acsr_weasel", "name": "ACSR Weasel (50 mm²)"}
    ]
    return jsonify({'conductor_types': conductor_types})

# ==================== DASHBOARD STATS API ====================

@app.route('/api/dashboard/stats')
@login_required
def api_dashboard_stats():
    """Get dashboard statistics based on user's role"""
    try:
        user = get_current_user()
        projects = ProjectDB.get_projects(user['id'])
        
        stats = {
            'total': len(projects),
            'by_type': {},
            'by_priority': {},
            'by_status': {},
            'by_division': {},
            'total_progress': 0,
            'user_role': user['role'],
            'user_division': user.get('division', 'All')
        }
        
        for p in projects:
            # Count by type
            p_type = p.get('project_type', 'Other')
            stats['by_type'][p_type] = stats['by_type'].get(p_type, 0) + 1
            
            # Count by priority
            priority = p.get('priority', 'Other')
            stats['by_priority'][priority] = stats['by_priority'].get(priority, 0) + 1
            
            # Count by status
            status = p.get('status', 'unknown')
            stats['by_status'][status] = stats['by_status'].get(status, 0) + 1
            
            # Count by division
            division = p.get('division', 'Other')
            stats['by_division'][division] = stats['by_division'].get(division, 0) + 1
            
            # Sum progress
            stats['total_progress'] += p.get('progress_percentage', 0)
        
        if stats['total'] > 0:
            stats['average_progress'] = round(stats['total_progress'] / stats['total'], 1)
        
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== TEST ROUTES ====================

@app.route('/my-role')
@login_required
def my_role():
    """Simple page to check user role (for testing)"""
    user = session['user']
    return f"""
    <h2>Your Information</h2>
    <p>Email: {user['email']}</p>
    <p>Full Name: {user.get('full_name', 'Not set')}</p>
    <p>Role: {user['role']}</p>
    <p>Region: {user.get('region', 'Not set')}</p>
    <p>Division: {user.get('division', 'Not set')}</p>
    <p>Section: {user.get('section', 'Not set')}</p>
    <p>User ID: {user['id']}</p>
    <br>
    <a href="/priority-works">Back to Dashboard</a>
    """

# ==================== FILTER TEST PAGE ====================

@app.route('/filter-test')
@login_required
def filter_test():
    """Test page for filters (for development)"""
    user = session['user']
    return render_template('filter_test.html', user=user)

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