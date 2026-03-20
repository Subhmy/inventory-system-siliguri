"""
API Routes for data fetching
Complete version with all endpoints
Last Updated: March 13, 2026
"""

from flask import Blueprint, jsonify, session, request
from app.utils.decorators import login_required
from app.models.mongo_utils import get_db

api_bp = Blueprint('api', __name__)

# ==================== FILTER OPTIONS ENDPOINTS ====================

@api_bp.route('/api/filter-options')
@login_required
def get_filter_options():
    """Get filter dropdown options for all dashboards"""
    try:
        db = get_db()
        if db is None:
            return jsonify({
                "zones": [{"id": "zone_siliguri", "name": "Siliguri Zone"}],
                "regions": [],
                "divisions": []
            }), 200
        
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
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/zones')
@login_required
def get_zones():
    """Get all zones with cumulative totals"""
    try:
        db = get_db()
        if db is None:
            return jsonify([{
                "_id": "zone_siliguri",
                "name": "Siliguri Zone",
                "incharge": "Chief Engineer",
                "total_consumers": 0,
                "total_staff": 0,
                "total_dtr": 0,
                "center_count": 0
            }]), 200
        
        centers = list(db.centers.find({}))
        
        if centers:
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
        return jsonify({"error": str(e)}), 500

# ==================== REGION ENDPOINTS ====================

@api_bp.route('/api/regions')
@login_required
def get_regions():
    """Get all regions"""
    try:
        db = get_db()
        if db is None:
            return jsonify([]), 200
            
        centers = list(db.centers.find({}))
        regions = list(set(c.get('region') for c in centers if c.get('region')))
        regions.sort()
        
        return jsonify([{"id": r, "name": r} for r in regions]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/region/<region_id>')
@login_required
def get_region_details(region_id):
    """Get detailed information for a specific region"""
    try:
        db = get_db()
        if db is None:
            return jsonify({"error": "Database not connected"}), 500
        
        centers = list(db.centers.find({"region": region_id}))
        
        return jsonify({
            "name": region_id,
            "type": "region",
            "total_centers": len(centers),
            "total_consumers": sum(c.get('total_consumers', 0) for c in centers),
            "total_staff": sum(c.get('total_staff', 0) for c in centers),
            "total_dtr": sum(c.get('total_dtr', 0) for c in centers),
            "centers": centers
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==================== DIVISION ENDPOINTS ====================

@api_bp.route('/api/divisions')
@login_required
def get_divisions():
    """Get all divisions"""
    try:
        db = get_db()
        if db is None:
            return jsonify([]), 200
            
        centers = list(db.centers.find({}))
        divisions = list(set(c.get('division') for c in centers if c.get('division')))
        divisions.sort()
        
        return jsonify([{"id": d, "name": d} for d in divisions]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/division/<division_id>')
@login_required
def get_division_details(division_id):
    """Get detailed information for a specific division"""
    try:
        db = get_db()
        if db is None:
            return jsonify({"error": "Database not connected"}), 500
        
        centers = list(db.centers.find({"division": division_id}))
        
        return jsonify({
            "name": division_id,
            "type": "division",
            "total_centers": len(centers),
            "total_consumers": sum(c.get('total_consumers', 0) for c in centers),
            "total_staff": sum(c.get('total_staff', 0) for c in centers),
            "total_dtr": sum(c.get('total_dtr', 0) for c in centers),
            "centers": centers
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==================== CENTER ENDPOINTS ====================

@api_bp.route('/api/centers')
@login_required
def get_centers():
    """Get all centers"""
    try:
        db = get_db()
        if db is None:
            return jsonify([]), 200
            
        centers = list(db.centers.find({}))
        for center in centers:
            center['_id'] = str(center['_id'])
        
        return jsonify(centers), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/center/<center_name>')
@login_required
def get_center_details(center_name):
    """Get detailed information for a specific center"""
    try:
        db = get_db()
        if db is None:
            return jsonify({"error": "Database not connected"}), 500
        
        center = db.centers.find_one({"name": center_name})
        if center:
            center['_id'] = str(center['_id'])
            return jsonify(center), 200
        else:
            return jsonify({"error": "Center not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==================== ADMIN DASHBOARD ENDPOINTS ====================

@api_bp.route('/api/admin/data', methods=['POST'])
@login_required
def get_admin_data():
    """Get admin dashboard data with filters"""
    try:
        filters = request.json or {}
        db = get_db()
        
        if db is None:
            return jsonify({
                'kpi': {'regions': 0, 'divisions': 0, 'substations': 0, 'staff': 0, 'consumers': 0},
                'regions': [],
                'divisions': []
            }), 200
        
        # Build query based on filters
        query = {}
        if filters.get('zone') and filters['zone'] != 'all':
            # Zone filter logic here if needed
            pass
        if filters.get('region') and filters['region'] != 'all':
            query['region'] = filters['region']
        if filters.get('division') and filters['division'] != 'all':
            query['division'] = filters['division']
        
        centers = list(db.centers.find(query))
        
        # Calculate KPIs
        unique_regions = len(set(c.get('region') for c in centers if c.get('region')))
        unique_divisions = len(set(c.get('division') for c in centers if c.get('division')))
        total_dtr = sum(c.get('total_dtr', 0) for c in centers)
        total_staff = sum(c.get('total_staff', 0) for c in centers)
        total_consumers = sum(c.get('total_consumers', 0) for c in centers)
        
        # Format regions data
        region_dict = {}
        for center in centers:
            region = center.get('region')
            if region:
                if region not in region_dict:
                    region_dict[region] = {
                        'name': region,
                        'divisions': set(),
                        'substations': 0,
                        'staff': 0,
                        'incharge': 'Regional Manager',
                        'contact': 'N/A'
                    }
                region_dict[region]['divisions'].add(center.get('division'))
                region_dict[region]['substations'] += center.get('total_dtr', 0)
                region_dict[region]['staff'] += center.get('total_staff', 0)
        
        regions_data = []
        for region_name, data in region_dict.items():
            regions_data.append({
                'name': region_name,
                'divisions': len(data['divisions']),
                'substations': data['substations'],
                'staff': data['staff'],
                'incharge': data['incharge'],
                'contact': data['contact']
            })
        
        # Format divisions data
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
                'tech': center.get('total_staff', 0)
            })
            sl += 1
        
        return jsonify({
            'kpi': {
                'regions': unique_regions,
                'divisions': unique_divisions,
                'substations': total_dtr,
                'staff': total_staff,
                'consumers': total_consumers
            },
            'regions': regions_data,
            'divisions': divisions_data
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==================== TECHNICAL DASHBOARD ENDPOINTS ====================

@api_bp.route('/api/technical/data', methods=['POST'])
@login_required
def get_technical_data():
    """Get technical dashboard data"""
    try:
        filters = request.json or {}
        db = get_db()
        
        if db is None:
            # Return sample data if no database
            return jsonify({
                'kpi': {
                    'ptr_units': 48,
                    'ptr_capacity': 520,
                    'dtr_units': 845,
                    'dtr_capacity': 42.5,
                    'line_33kv': 78.5,
                    'towers': 245,
                    'feeders_11kv': 86,
                    'feeder_length': 486
                }
            }), 200
        
        # Fetch data from collections
        substations = list(db.substation_33_11kv.find({}))
        lines33kv = list(db.line_33kv.find({}))
        lines11kv = list(db.line_11kv.find({}))
        
        data = {
            'kpi': {
                'ptr_units': len(substations),
                'ptr_capacity': sum(s.get('capacity_mva', 0) for s in substations),
                'dtr_units': sum(s.get('dtr_count', 0) for s in substations),
                'dtr_capacity': 42.5,  # Placeholder
                'line_33kv': sum(l.get('length_km', 0) for l in lines33kv),
                'towers': sum(l.get('towers', 0) for l in lines33kv),
                'feeders_11kv': len(lines11kv),
                'feeder_length': sum(l.get('length_km', 0) for l in lines11kv)
            }
        }
        
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==================== COMMERCIAL DASHBOARD ENDPOINTS ====================

@api_bp.route('/api/commercial/data', methods=['POST'])
@login_required
def get_commercial_data():
    """Get commercial dashboard data"""
    try:
        filters = request.json or {}
        
        # Sample commercial data
        data = {
            'kpi': {
                'input': 185.6,
                'demand': 168.2,
                'collection': 42.5,
                'collection_eff': 94.2
            },
            'summary': {
                'atc_loss': 18.5,
                'atc_loss_change': 2.1,
                'td_loss': 12.3,
                'td_loss_change': -1.2,
                'collection_eff': 94.2,
                'collection_eff_change': 3.1,
                'outstanding': 8.2,
                'outstanding_change': 0.5
            }
        }
        
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==================== OFFICE DETAILS ENDPOINT ====================

@api_bp.route('/api/office-details')
@login_required
def get_office_details():
    """Get detailed information for a specific office (zone/region/division/center)"""
    try:
        office_type = request.args.get('type')
        office_id = request.args.get('id')
        
        db = get_db()
        
        if db is None:
            return jsonify({"error": "Database not connected"}), 500
        
        if office_type == 'zone':
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
            center = db.centers.find_one({"name": office_id})
            if center:
                center['_id'] = str(center['_id'])
                return jsonify(center), 200
            else:
                return jsonify({"error": "Center not found"}), 404
        else:
            return jsonify({"error": "Invalid office type"}), 400
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==================== PRIORITY WORKS API ENDPOINTS ====================

@api_bp.route('/api/priority-works/overview')
@login_required
def get_priority_works_overview():
    """Get data for Priority Works Overview cards"""
    try:
        db = get_db()
        if db is None:
            # Return default data if no database connection
            data = {
                'hvds': {'total': 8, 'completed': 3, 'in_progress': 5, 'percentage': '65%', 'budget': '₹8.2Cr'},
                'newSubstation': {'total': 3, 'capacity': '120 MVA', 'progress': '45%', 'budget': '₹6.5Cr', 'target': 'Dec 2026'},
                'ptr': {'total': 12, 'completed': 5, 'capacity': '85 MVA', 'progress': '42%', 'budget': '₹4.8Cr'},
                'new33kv': {'count': 2, 'length': '28 km', 'towers': 84, 'budget': '₹3.2Cr', 'start': 'Apr 2026'},
                'cond33kv': {'count': 8, 'length': '42 km', 'completed': 3, 'progress': '38%', 'budget': '₹2.8Cr'},
                'new11kv': {'count': 5, 'length': '15 km', 'poles': 225, 'budget': '₹1.8Cr', 'start': 'May 2026'},
                'cond11kv': {'count': 12, 'length': '38 km', 'completed': 12, 'progress': '100%', 'budget': '₹1.2Cr'}
            }
            return jsonify(data), 200
            
        # TODO: Fetch from MongoDB collections when ready
        # For now, return default data
        data = {
            'hvds': {'total': 8, 'completed': 3, 'in_progress': 5, 'percentage': '65%', 'budget': '₹8.2Cr'},
            'newSubstation': {'total': 3, 'capacity': '120 MVA', 'progress': '45%', 'budget': '₹6.5Cr', 'target': 'Dec 2026'},
            'ptr': {'total': 12, 'completed': 5, 'capacity': '85 MVA', 'progress': '42%', 'budget': '₹4.8Cr'},
            'new33kv': {'count': 2, 'length': '28 km', 'towers': 84, 'budget': '₹3.2Cr', 'start': 'Apr 2026'},
            'cond33kv': {'count': 8, 'length': '42 km', 'completed': 3, 'progress': '38%', 'budget': '₹2.8Cr'},
            'new11kv': {'count': 5, 'length': '15 km', 'poles': 225, 'budget': '₹1.8Cr', 'start': 'May 2026'},
            'cond11kv': {'count': 12, 'length': '38 km', 'completed': 12, 'progress': '100%', 'budget': '₹1.2Cr'}
        }
        
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==================== DASHBOARD ADMINISTRATIVE API ====================

# ==================== DETAILED DASHBOARD API ENDPOINTS ====================

@api_bp.route('/api/substation/data', methods=['POST'])
@login_required
def get_substation_data():
    """Get detailed substation dashboard data"""
    try:
        db = get_db()
        filters = request.json or {}
        
        if db is None:
            return jsonify({
                'kpi': {
                    'total_substations': 24,
                    'total_ptrs': 48,
                    'total_dtrs': 845,
                    'total_capacity': 520,
                    'active_substations': 22,
                    'under_maintenance': 2
                },
                'substations': []
            }), 200
        
        query = {}
        if filters.get('region') and filters['region'] != 'all':
            query['region'] = filters['region']
        if filters.get('division') and filters['division'] != 'all':
            query['division'] = filters['division']
        
        substations = list(db.substation_33_11kv.find(query))
        
        for sub in substations:
            sub['_id'] = str(sub['_id'])
        
        total_ptrs = sum(s.get('ptr_count', 0) for s in substations)
        total_dtrs = sum(s.get('dtr_count', 0) for s in substations)
        total_capacity = sum(s.get('capacity_mva', 0) for s in substations)
        
        return jsonify({
            'kpi': {
                'total_substations': len(substations),
                'total_ptrs': total_ptrs,
                'total_dtrs': total_dtrs,
                'total_capacity': total_capacity,
                'active_substations': len([s for s in substations if s.get('status') == 'Active']),
                'under_maintenance': len([s for s in substations if s.get('status') == 'Maintenance'])
            },
            'substations': substations
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/line-33kv/data', methods=['POST'])
@login_required
def get_line_33kv_data():
    """Get detailed 33kV line dashboard data"""
    try:
        db = get_db()
        filters = request.json or {}
        
        if db is None:
            return jsonify({
                'kpi': {
                    'total_lines': 18,
                    'total_length': 78.5,
                    'total_towers': 245,
                    'avg_length': 4.36,
                    'augmentation_progress': 45,
                    'lines_completed': 3
                },
                'lines': []
            }), 200
        
        query = {}
        if filters.get('region') and filters['region'] != 'all':
            query['region'] = filters['region']
        if filters.get('division') and filters['division'] != 'all':
            query['division'] = filters['division']
        
        lines = list(db.line_33kv.find(query))
        
        for line in lines:
            line['_id'] = str(line['_id'])
        
        total_length = sum(l.get('length_km', 0) for l in lines)
        total_towers = sum(l.get('towers', 0) for l in lines)
        
        return jsonify({
            'kpi': {
                'total_lines': len(lines),
                'total_length': total_length,
                'total_towers': total_towers,
                'avg_length': round(total_length / len(lines), 2) if lines else 0,
                'augmentation_progress': 45,
                'lines_completed': len([l for l in lines if l.get('status') == 'Completed'])
            },
            'lines': lines
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/line-11kv/data', methods=['POST'])
@login_required
def get_line_11kv_data():
    """Get detailed 11kV line dashboard data"""
    try:
        db = get_db()
        filters = request.json or {}
        
        if db is None:
            return jsonify({
                'kpi': {
                    'total_feeders': 42,
                    'total_length': 486,
                    'total_poles': 1250,
                    'avg_length': 11.57,
                    'operational_feeders': 38,
                    'under_maintenance': 4
                },
                'feeders': []
            }), 200
        
        query = {}
        if filters.get('region') and filters['region'] != 'all':
            query['region'] = filters['region']
        if filters.get('division') and filters['division'] != 'all':
            query['division'] = filters['division']
        
        feeders = list(db.line_11kv.find(query))
        
        for feeder in feeders:
            feeder['_id'] = str(feeder['_id'])
        
        total_length = sum(f.get('length_km', 0) for f in feeders)
        total_poles = sum(f.get('poles', 0) for f in feeders)
        
        return jsonify({
            'kpi': {
                'total_feeders': len(feeders),
                'total_length': total_length,
                'total_poles': total_poles,
                'avg_length': round(total_length / len(feeders), 2) if feeders else 0,
                'operational_feeders': len([f for f in feeders if f.get('status') == 'Operational']),
                'under_maintenance': len([f for f in feeders if f.get('status') == 'Maintenance'])
            },
            'feeders': feeders
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/hvds/data', methods=['POST'])
@login_required
def get_hvds_data():
    """Get detailed HVDS dashboard data"""
    try:
        data = {
            'kpi': {
                'total_projects': 8,
                'completed': 3,
                'in_progress': 5,
                'total_budget': 8.2,
                'progress': 65,
                'target_date': 'Dec 2026'
            },
            'projects': [
                {'name': 'Siliguri HVDS Phase 1', 'status': 'Completed', 'progress': 100, 'budget': 1.2},
                {'name': 'Siliguri HVDS Phase 2', 'status': 'In Progress', 'progress': 75, 'budget': 1.5},
                {'name': 'Jalpaiguri HVDS', 'status': 'In Progress', 'progress': 60, 'budget': 1.8},
                {'name': 'Coochbehar HVDS', 'status': 'In Progress', 'progress': 45, 'budget': 1.2},
                {'name': 'Alipurduar HVDS', 'status': 'In Progress', 'progress': 30, 'budget': 0.9},
                {'name': 'Darjeeling HVDS', 'status': 'Planned', 'progress': 0, 'budget': 1.6}
            ]
        }
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/new-substation/data', methods=['POST'])
@login_required
def get_new_substation_data():
    """Get detailed new substation projects data"""
    try:
        data = {
            'kpi': {
                'total_projects': 3,
                'total_capacity': 120,
                'completed': 0,
                'in_progress': 3,
                'progress': 45,
                'total_budget': 6.5
            },
            'projects': [
                {'name': 'Siliguri New SS', 'capacity': 40, 'status': 'In Progress', 'progress': 50, 'budget': 2.2},
                {'name': 'Jalpaiguri New SS', 'capacity': 40, 'status': 'In Progress', 'progress': 40, 'budget': 2.1},
                {'name': 'Coochbehar New SS', 'capacity': 40, 'status': 'In Progress', 'progress': 45, 'budget': 2.2}
            ]
        }
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/ptr/data', methods=['POST'])
@login_required
def get_ptr_data():
    """Get detailed PTR augmentation data"""
    try:
        data = {
            'kpi': {
                'total_units': 12,
                'completed': 5,
                'in_progress': 7,
                'capacity_increase': 85,
                'progress': 42,
                'total_budget': 4.8
            },
            'projects': [
                {'name': 'City Center PTR', 'capacity': 10, 'status': 'Completed', 'progress': 100, 'budget': 0.4},
                {'name': 'Siliguri Town PTR', 'capacity': 8, 'status': 'In Progress', 'progress': 70, 'budget': 0.5},
                {'name': 'Jalpaiguri PTR', 'capacity': 12, 'status': 'In Progress', 'progress': 45, 'budget': 0.6}
            ]
        }
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/new-33kv-line/data', methods=['POST'])
@login_required
def get_new_33kv_line_data():
    """Get detailed new 33kV line projects data"""
    try:
        data = {
            'kpi': {
                'total_lines': 2,
                'total_length': 28,
                'total_towers': 84,
                'progress': 15,
                'total_budget': 3.2
            },
            'projects': [
                {'name': 'Siliguri-Bagdogra 33kV', 'length': 15, 'towers': 45, 'status': 'Planned', 'progress': 10, 'budget': 1.8},
                {'name': 'Jalpaiguri-Mal 33kV', 'length': 13, 'towers': 39, 'status': 'Planned', 'progress': 20, 'budget': 1.4}
            ]
        }
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/conductor-33kv/data', methods=['POST'])
@login_required
def get_conductor_33kv_data():
    """Get detailed 33kV conductor augmentation data"""
    try:
        data = {
            'kpi': {
                'total_sections': 8,
                'total_length': 42,
                'completed': 3,
                'in_progress': 5,
                'progress': 38,
                'total_budget': 2.8
            },
            'sections': [
                {'name': 'Siliguri Section', 'length': 5.2, 'status': 'Completed', 'progress': 100, 'budget': 0.35},
                {'name': 'Kurseong Section', 'length': 4.8, 'status': 'Completed', 'progress': 100, 'budget': 0.32},
                {'name': 'Darjeeling Section', 'length': 6.5, 'status': 'In Progress', 'progress': 65, 'budget': 0.45}
            ]
        }
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/new-11kv-line/data', methods=['POST'])
@login_required
def get_new_11kv_line_data():
    """Get detailed new 11kV line projects data"""
    try:
        data = {
            'kpi': {
                'total_feeders': 5,
                'total_length': 15,
                'total_poles': 225,
                'progress': 10,
                'total_budget': 1.8
            },
            'projects': [
                {'name': 'Siliguri Industrial Feeder', 'length': 3.2, 'poles': 48, 'status': 'Planned', 'progress': 5, 'budget': 0.38},
                {'name': 'Jalpaiguri Township', 'length': 2.8, 'poles': 42, 'status': 'Planned', 'progress': 8, 'budget': 0.34}
            ]
        }
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/conductor-11kv/data', methods=['POST'])
@login_required
def get_conductor_11kv_data():
    """Get detailed 11kV conductor augmentation data"""
    try:
        data = {
            'kpi': {
                'total_sections': 12,
                'total_length': 38,
                'completed': 12,
                'in_progress': 0,
                'progress': 100,
                'total_budget': 1.2
            },
            'sections': [
                {'name': 'Siliguri Town Section', 'length': 3.5, 'status': 'Completed', 'progress': 100, 'budget': 0.11},
                {'name': 'Jalpaiguri Section', 'length': 4.2, 'status': 'Completed', 'progress': 100, 'budget': 0.13},
                {'name': 'Mal Section', 'length': 2.8, 'status': 'Completed', 'progress': 100, 'budget': 0.09}
            ]
        }
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500