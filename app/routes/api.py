"""
API Routes - Using consumption_summary collection
Consumption data is pre-aggregated from transactions_summary
Last Updated: June 18, 2026 - FIXED: Plant-level monthly average for Stock Position
ADDED: Material in Transit endpoints (DI and STN) with plant code extraction
"""

from flask import Blueprint, jsonify, session, request
from app.utils.decorators import login_required
from app.models.mongo_utils import get_db
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import re

api_bp = Blueprint('api', __name__)

# ==================== HELPER FUNCTIONS ====================

def safe_json_response(data, status=200):
    try:
        return jsonify(data), status
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def parse_period(period_str):
    """Parse period string to extract date components"""
    try:
        if '/' in period_str:
            parts = period_str.split('/')
            if len(parts) == 3:
                return {
                    'year': int(parts[2]),
                    'month': int(parts[0]),
                    'quarter': (int(parts[0]) - 1) // 3 + 1
                }
        elif '-' in period_str:
            parts = period_str.split('-')
            if len(parts) == 2:
                return {
                    'year': int(parts[0]),
                    'month': int(parts[1]),
                    'quarter': (int(parts[1]) - 1) // 3 + 1
                }
        return {'year': 2024, 'month': 1, 'quarter': 1}
    except:
        return {'year': 2024, 'month': 1, 'quarter': 1}


def extract_plant_code(value):
    """Extract 4-digit plant code from any string"""
    if not value:
        return ''
    val_str = str(value).strip()
    # Try to find 4-digit number first
    match = re.search(r'(\d{4})', val_str)
    if match:
        return match.group(1)
    # If no 4-digit found, try to find any number
    match = re.search(r'(\d+)', val_str)
    if match:
        return match.group(1)
    return ''


# ==================== EXISTING FILTER ENDPOINTS ====================

@api_bp.route('/api/filter-options')
@login_required
def get_filter_options():
    try:
        db = get_db()
        if db is None:
            return safe_json_response({"zones": [{"id": "zone_siliguri", "name": "Siliguri Zone"}], "regions": [], "divisions": []})
        
        centers = list(db.centers.find({}))
        regions = sorted({c.get('region') for c in centers if c.get('region')})
        divisions = sorted({c.get('division') for c in centers if c.get('division')})
        
        return safe_json_response({
            "zones": [{"id": "zone_siliguri", "name": "Siliguri Zone"}],
            "regions": [{"id": r, "name": r} for r in regions],
            "divisions": [{"id": d, "name": d} for d in divisions]
        })
    except Exception as e:
        return safe_json_response({"error": str(e)})


@api_bp.route('/api/zones')
@login_required
def get_zones():
    try:
        db = get_db()
        if db is None:
            return safe_json_response([{"_id": "zone_siliguri", "name": "Siliguri Zone", "incharge": "Chief Engineer", "total_consumers": 0, "total_staff": 0, "total_dtr": 0, "center_count": 0}])
        
        centers = list(db.centers.find({}))
        if centers:
            return safe_json_response([{
                "_id": "zone_siliguri",
                "name": "Siliguri Zone",
                "incharge": "Chief Engineer",
                "total_consumers": sum(c.get('total_consumers', 0) for c in centers),
                "total_staff": sum(c.get('total_staff', 0) for c in centers),
                "total_dtr": sum(c.get('total_dtr', 0) for c in centers),
                "center_count": len(centers)
            }])
        return safe_json_response([{"_id": "zone_siliguri", "name": "Siliguri Zone", "incharge": "Chief Engineer", "total_consumers": 0, "total_staff": 0, "total_dtr": 0, "center_count": 0}])
    except Exception as e:
        return safe_json_response({"error": str(e)})


@api_bp.route('/api/regions')
@login_required
def get_regions():
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        centers = list(db.centers.find({}))
        regions = sorted({c.get('region') for c in centers if c.get('region')})
        return safe_json_response([{"id": r, "name": r} for r in regions])
    except Exception as e:
        return safe_json_response({"error": str(e)})


@api_bp.route('/api/divisions')
@login_required
def get_divisions():
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        centers = list(db.centers.find({}))
        divisions = sorted({c.get('division') for c in centers if c.get('division')})
        return safe_json_response([{"id": d, "name": d} for d in divisions])
    except Exception as e:
        return safe_json_response({"error": str(e)})


@api_bp.route('/api/centers')
@login_required
def get_centers():
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        centers = list(db.centers.find({}))
        for c in centers:
            c['_id'] = str(c['_id'])
        return safe_json_response(centers)
    except Exception as e:
        return safe_json_response({"error": str(e)})


# ==================== ADMIN DASHBOARD ====================

@api_bp.route('/api/admin/data', methods=['POST'])
@login_required
def get_admin_data():
    try:
        filters = request.json or {}
        db = get_db()
        if db is None:
            return safe_json_response({'kpi': {'regions': 0, 'divisions': 0, 'substations': 0, 'staff': 0, 'consumers': 0}, 'regions': [], 'divisions': []})
        
        query = {}
        if filters.get('region') and filters['region'] != 'all':
            query['region'] = filters['region']
        if filters.get('division') and filters['division'] != 'all':
            query['division'] = filters['division']
        
        centers = list(db.centers.find(query))
        
        unique_regions = len({c.get('region') for c in centers if c.get('region')})
        unique_divisions = len({c.get('division') for c in centers if c.get('division')})
        total_dtr = sum(c.get('total_dtr', 0) for c in centers)
        total_staff = sum(c.get('total_staff', 0) for c in centers)
        total_consumers = sum(c.get('total_consumers', 0) for c in centers)
        
        region_dict = {}
        for c in centers:
            region = c.get('region')
            if region:
                if region not in region_dict:
                    region_dict[region] = {'name': region, 'divisions': set(), 'substations': 0, 'staff': 0, 'incharge': 'Regional Manager', 'contact': 'N/A'}
                region_dict[region]['divisions'].add(c.get('division'))
                region_dict[region]['substations'] += c.get('total_dtr', 0)
                region_dict[region]['staff'] += c.get('total_staff', 0)
        
        regions_data = [{'name': r, 'divisions': len(d['divisions']), 'substations': d['substations'], 'staff': d['staff'], 'incharge': d['incharge'], 'contact': d['contact']} for r, d in region_dict.items()]
        divisions_data = [{'sl': i+1, 'name': c.get('division', 'N/A'), 'region': c.get('region', 'N/A'), 'substations': c.get('total_dtr', 0), 'aed': c.get('incharge', 'N/A'), 'contact': c.get('incharge_contact', 'N/A'), 'email': c.get('incharge_email', 'N/A'), 'staff': c.get('total_staff', 0), 'tech': c.get('total_staff', 0)} for i, c in enumerate(centers)]
        
        return safe_json_response({'kpi': {'regions': unique_regions, 'divisions': unique_divisions, 'substations': total_dtr, 'staff': total_staff, 'consumers': total_consumers}, 'regions': regions_data, 'divisions': divisions_data})
    except Exception as e:
        return safe_json_response({"error": str(e)})


# ==================== CONSUMPTION ANALYSIS ENDPOINTS ====================

@api_bp.route('/api/consumption/overview')
@login_required
def get_consumption_overview():
    try:
        db = get_db()
        if db is None:
            return safe_json_response({'total_consumption': 0, 'avg_monthly': 0, 'top_material': 'No data', 'active_plants': 0})
        
        pipeline = [
            {'$group': {
                '_id': None,
                'total_consumption': {'$sum': '$quantity'},
                'unique_materials': {'$addToSet': '$material_code'},
            }}
        ]
        
        result = list(db.consumption_summary.aggregate(pipeline))
        
        if result and result[0].get('total_consumption', 0) > 0:
            total = result[0].get('total_consumption', 0)
            
            top_pipeline = [
                {'$group': {
                    '_id': {'code': '$material_code', 'name': '$material_name'},
                    'quantity': {'$sum': '$quantity'}
                }},
                {'$sort': {'quantity': -1}},
                {'$limit': 1}
            ]
            top_result = list(db.consumption_summary.aggregate(top_pipeline))
            top_material = top_result[0]['_id']['name'] if top_result else 'No data'
            
            return safe_json_response({
                'total_consumption': round(total, 2),
                'avg_monthly': round(total / 12, 2),
                'top_material': top_material,
                'active_plants': 12,
                'warnings': {'stock_only': 0, 'no_stock': 0}
            })
        
        return safe_json_response({
            'total_consumption': 0,
            'avg_monthly': 0,
            'top_material': 'No data',
            'active_plants': 0,
            'warnings': {'stock_only': 0, 'no_stock': 0}
        })
        
    except Exception as e:
        print(f"Error in get_consumption_overview: {e}")
        return safe_json_response({
            'total_consumption': 0,
            'avg_monthly': 0,
            'top_material': 'Error',
            'active_plants': 0
        })


@api_bp.route('/api/consumption/data', methods=['POST'])
@login_required
def get_consumption_data():
    """
    Get detailed consumption data with proper period-based aggregation.
    """
    try:
        data = request.json or {}
        period_type = data.get('period', 'monthly')
        plant = data.get('plant', 'all')
        material_group = data.get('material_group', 'all')
        material_code = data.get('material_code', 'all')
        
        db = get_db()
        if db is None:
            return safe_json_response({'consumption_data': [], 'summary': {'total_consumption': 0}})
        
        # Build query
        query = {}
        if plant and plant != 'all':
            query['plant'] = plant
        if material_group and material_group != 'all':
            query['material_group'] = material_group
        if material_code and material_code != 'all':
            query['material_code'] = material_code
        
        print(f"Query: {query}")
        
        all_data = list(db.consumption_summary.find(query))
        if not all_data:
            return safe_json_response({
                'consumption_data': [],
                'top_materials': [],
                'group_consumption': [],
                'plant_consumption': [],
                'summary': {'total_consumption': 0}
            })
        
        df = pd.DataFrame(all_data)
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        
        parsed_periods = df['period'].apply(parse_period)
        df['year'] = parsed_periods.apply(lambda x: x['year'])
        df['month'] = parsed_periods.apply(lambda x: x['month'])
        df['quarter'] = parsed_periods.apply(lambda x: x['quarter'])
        
        if period_type == 'quarterly':
            df['period_key'] = df['year'].astype(str) + '-Q' + df['quarter'].astype(str)
            trend_agg = df.groupby('period_key').agg({'quantity': 'sum'}).reset_index()
            trend_agg = trend_agg.sort_values('period_key')
            consumption_data = [{'period': row['period_key'], 'quantity': round(row['quantity'], 2)} 
                              for _, row in trend_agg.iterrows()]
            quarterly_df = df.groupby(['period_key', 'material_code', 'material_name', 'unit', 'material_group', 'plant']).agg({'quantity': 'sum'}).reset_index()
            top_agg = quarterly_df.groupby(['material_code', 'material_name', 'unit']).agg({'quantity': 'sum'}).reset_index()
            top_agg = top_agg.sort_values('quantity', ascending=False).head(15)
            top_materials = [{
                'material_code': str(row['material_code']),
                'material_name': row['material_name'] or 'Unknown',
                'quantity': round(row['quantity'], 2),
                'unit': row['unit'] or 'Units'
            } for _, row in top_agg.iterrows()]
            group_agg = quarterly_df.groupby('material_group').agg({'quantity': 'sum'}).reset_index()
            group_agg = group_agg.sort_values('quantity', ascending=False)
            group_consumption = [{
                'material_group': row['material_group'] or 'Uncategorized',
                'quantity': round(row['quantity'], 2)
            } for _, row in group_agg.iterrows()]
            plant_agg = quarterly_df.groupby('plant').agg({'quantity': 'sum'}).reset_index()
            plant_consumption = [{
                'plant': row['plant'] or 'Unknown',
                'quantity': round(row['quantity'], 2)
            } for _, row in plant_agg.iterrows() if row['plant'] and row['plant'] != 'all']
            total_consumption = quarterly_df['quantity'].sum()
            
        elif period_type == 'yearly':
            df['period_key'] = df['year'].astype(str)
            trend_agg = df.groupby('year').agg({'quantity': 'sum'}).reset_index()
            trend_agg = trend_agg.sort_values('year')
            consumption_data = [{'period': str(row['year']), 'quantity': round(row['quantity'], 2)} 
                              for _, row in trend_agg.iterrows()]
            yearly_df = df.groupby(['year', 'material_code', 'material_name', 'unit', 'material_group', 'plant']).agg({'quantity': 'sum'}).reset_index()
            top_agg = yearly_df.groupby(['material_code', 'material_name', 'unit']).agg({'quantity': 'sum'}).reset_index()
            top_agg = top_agg.sort_values('quantity', ascending=False).head(15)
            top_materials = [{
                'material_code': str(row['material_code']),
                'material_name': row['material_name'] or 'Unknown',
                'quantity': round(row['quantity'], 2),
                'unit': row['unit'] or 'Units'
            } for _, row in top_agg.iterrows()]
            group_agg = yearly_df.groupby('material_group').agg({'quantity': 'sum'}).reset_index()
            group_agg = group_agg.sort_values('quantity', ascending=False)
            group_consumption = [{
                'material_group': row['material_group'] or 'Uncategorized',
                'quantity': round(row['quantity'], 2)
            } for _, row in group_agg.iterrows()]
            plant_agg = yearly_df.groupby('plant').agg({'quantity': 'sum'}).reset_index()
            plant_consumption = [{
                'plant': row['plant'] or 'Unknown',
                'quantity': round(row['quantity'], 2)
            } for _, row in plant_agg.iterrows() if row['plant'] and row['plant'] != 'all']
            total_consumption = yearly_df['quantity'].sum()
            
        else:
            trend_agg = df.groupby('period').agg({'quantity': 'sum'}).reset_index()
            trend_agg = trend_agg.sort_values('period')
            consumption_data = [{'period': row['period'], 'quantity': round(row['quantity'], 2)} 
                              for _, row in trend_agg.iterrows()]
            top_agg = df.groupby(['material_code', 'material_name', 'unit']).agg({'quantity': 'sum'}).reset_index()
            top_agg = top_agg.sort_values('quantity', ascending=False).head(15)
            top_materials = [{
                'material_code': str(row['material_code']),
                'material_name': row['material_name'] or 'Unknown',
                'quantity': round(row['quantity'], 2),
                'unit': row['unit'] or 'Units'
            } for _, row in top_agg.iterrows()]
            group_agg = df.groupby('material_group').agg({'quantity': 'sum'}).reset_index()
            group_agg = group_agg.sort_values('quantity', ascending=False)
            group_consumption = [{
                'material_group': row['material_group'] or 'Uncategorized',
                'quantity': round(row['quantity'], 2)
            } for _, row in group_agg.iterrows()]
            plant_agg = df.groupby('plant').agg({'quantity': 'sum'}).reset_index()
            plant_consumption = [{
                'plant': row['plant'] or 'Unknown',
                'quantity': round(row['quantity'], 2)
            } for _, row in plant_agg.iterrows() if row['plant'] and row['plant'] != 'all']
            total_consumption = df['quantity'].sum()
        
        response = {
            'consumption_data': consumption_data,
            'top_materials': top_materials,
            'group_consumption': group_consumption,
            'plant_consumption': plant_consumption,
            'summary': {
                'total_consumption': round(total_consumption, 2),
                'period_type': period_type,
                'material_group': material_group,
                'material_code': material_code,
                'plant': plant
            }
        }
        
        return safe_json_response(response)
        
    except Exception as e:
        print(f"Error in get_consumption_data: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response({
            'consumption_data': [],
            'top_materials': [],
            'group_consumption': [],
            'plant_consumption': [],
            'summary': {'total_consumption': 0}
        })


@api_bp.route('/api/consumption/plants')
@login_required
def get_consumption_plants():
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        plants = db.consumption_summary.distinct('plant')
        plants = [p for p in plants if p and p != 'all']
        plant_list = [{'code': p, 'name': p} for p in sorted(plants)]
        
        return safe_json_response(plant_list)
    except Exception as e:
        print(f"Error in get_consumption_plants: {e}")
        return safe_json_response([])


@api_bp.route('/api/consumption/material-groups')
@login_required
def get_consumption_material_groups():
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        groups = db.consumption_summary.distinct('material_group')
        # Filter out unwanted groups
        excluded = ['monthly=', 'monthly=27', 'quarterly=3', 'qterly=3', 'yearly']
        groups = [g for g in groups if g and not any(ex in g.lower() for ex in excluded)]
        
        return safe_json_response(sorted(groups))
    except Exception as e:
        print(f"Error in get_consumption_material_groups: {e}")
        return safe_json_response([])


@api_bp.route('/api/consumption/materials')
@login_required
def get_consumption_materials():
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        group = request.args.get('group', None)
        
        query = {}
        if group and group != 'all' and group != 'undefined':
            query['material_group'] = group
        
        pipeline = [
            {'$match': query} if query else {'$match': {}},
            {'$match': {'material_code': {'$exists': True, '$ne': None, '$ne': ''}}},
            {'$group': {
                '_id': {
                    'code': '$material_code',
                    'name': '$material_name',
                    'group': '$material_group',
                    'unit': '$unit'
                }
            }},
            {'$sort': {'_id.name': 1}}
        ]
        
        materials_raw = list(db.consumption_summary.aggregate(pipeline))
        
        materials = []
        for m in materials_raw:
            if m['_id']['code']:
                materials.append({
                    'material_code': str(m['_id']['code']),
                    'material_name': m['_id']['name'] or str(m['_id']['code']),
                    'material_group': m['_id']['group'] or 'Uncategorized',
                    'unit': m['_id']['unit'] or 'Units'
                })
        
        # Filter out excluded groups
        excluded = ['monthly=', 'monthly=27', 'quarterly=3', 'qterly=3', 'yearly']
        materials = [m for m in materials if not any(ex in m.get('material_group', '').lower() for ex in excluded)]
        
        return safe_json_response(materials[:1000])
        
    except Exception as e:
        print(f"Error in get_consumption_materials: {e}")
        return safe_json_response([])


# ==================== MONTHLY AVERAGE CONSUMPTION ENDPOINTS ====================

@api_bp.route('/api/consumption/monthly-average')
@login_required
def get_monthly_average_consumption():
    """
    Get monthly average consumption per material for a specific plant
    """
    try:
        plant = request.args.get('plant', 'all')
        material_group = request.args.get('material_group', 'all')
        material_code = request.args.get('material_code', 'all')
        
        db = get_db()
        if db is None:
            return safe_json_response({})
        
        query = {'period_type': 'monthly'}
        
        if plant and plant != 'all' and plant != 'undefined':
            query['plant'] = plant
        if material_group and material_group != 'all' and material_group != 'undefined':
            query['material_group'] = material_group
        if material_code and material_code != 'all' and material_code != 'undefined':
            query['material_code'] = material_code
        
        print(f"Monthly Average Query: {query}")
        
        pipeline = [
            {'$match': query},
            {'$group': {
                '_id': {
                    'material_code': '$material_code',
                    'material_name': '$material_name',
                    'unit': '$unit',
                    'material_group': '$material_group',
                    'plant': '$plant'
                },
                'total_consumption': {'$sum': '$quantity'},
                'month_count': {'$sum': 1},
                'monthly_avg': {'$avg': '$quantity'}
            }},
            {'$project': {
                'material_code': '$_id.material_code',
                'material_name': '$_id.material_name',
                'unit': '$_id.unit',
                'material_group': '$_id.material_group',
                'plant': '$_id.plant',
                'total_consumption': 1,
                'month_count': 1,
                'monthly_avg': 1
            }}
        ]
        
        results = list(db.consumption_summary.aggregate(pipeline))
        print(f"Found {len(results)} monthly average records")
        
        avg_map = {}
        for r in results:
            material_code_key = str(r['material_code'])
            plant_key = str(r.get('plant', 'all'))
            
            if material_code_key not in avg_map:
                avg_map[material_code_key] = {}
            
            avg_map[material_code_key][plant_key] = {
                'monthly_avg': round(r['monthly_avg'], 2),
                'total_consumption': round(r['total_consumption'], 2),
                'month_count': r['month_count'],
                'material_name': r['material_name'],
                'unit': r['unit'],
                'material_group': r['material_group'],
                'plant': plant_key
            }
        
        # Also add zone-level averages as fallback
        zone_query = {'period_type': 'monthly'}
        if material_group and material_group != 'all':
            zone_query['material_group'] = material_group
        if material_code and material_code != 'all':
            zone_query['material_code'] = material_code
        
        zone_pipeline = [
            {'$match': zone_query},
            {'$group': {
                '_id': {
                    'material_code': '$material_code',
                    'material_name': '$material_name',
                    'unit': '$unit',
                    'material_group': '$material_group'
                },
                'total_consumption': {'$sum': '$quantity'},
                'month_count': {'$sum': 1},
                'monthly_avg': {'$avg': '$quantity'}
            }},
            {'$project': {
                'material_code': '$_id.material_code',
                'material_name': '$_id.material_name',
                'unit': '$_id.unit',
                'material_group': '$_id.material_group',
                'total_consumption': 1,
                'month_count': 1,
                'monthly_avg': 1
            }}
        ]
        
        zone_results = list(db.consumption_summary.aggregate(zone_pipeline))
        
        for r in zone_results:
            material_code_key = str(r['material_code'])
            if material_code_key not in avg_map:
                avg_map[material_code_key] = {}
            avg_map[material_code_key]['all'] = {
                'monthly_avg': round(r['monthly_avg'], 2),
                'total_consumption': round(r['total_consumption'], 2),
                'month_count': r['month_count'],
                'material_name': r['material_name'],
                'unit': r['unit'],
                'material_group': r['material_group'],
                'plant': 'all'
            }
        
        print(f"Returning averages for {len(avg_map)} materials")
        return safe_json_response(avg_map)
        
    except Exception as e:
        print(f"Error in get_monthly_average_consumption: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response({})


@api_bp.route('/api/consumption/monthly-average-by-plant')
@login_required
def get_monthly_average_by_plant():
    """
    Get monthly average consumption per material for MULTIPLE plants.
    """
    try:
        plants_param = request.args.get('plants', '')
        material_group = request.args.get('material_group', 'all')
        material_code = request.args.get('material_code', 'all')
        
        plants_list = []
        if plants_param and plants_param != 'all':
            plants_list = [p.strip() for p in plants_param.split(',') if p.strip()]
        
        db = get_db()
        if db is None:
            return safe_json_response({})
        
        query = {'period_type': 'monthly'}
        
        if plants_list:
            query['plant'] = {'$in': plants_list}
        if material_group and material_group != 'all' and material_group != 'undefined':
            query['material_group'] = material_group
        if material_code and material_code != 'all' and material_code != 'undefined':
            query['material_code'] = material_code
        
        print(f"Monthly Average By Plant Query: {query}")
        print(f"Plants list: {plants_list}")
        
        pipeline = [
            {'$match': query},
            {'$group': {
                '_id': {
                    'material_code': '$material_code',
                    'material_name': '$material_name',
                    'unit': '$unit',
                    'material_group': '$material_group',
                    'plant': '$plant'
                },
                'total_consumption': {'$sum': '$quantity'},
                'month_count': {'$sum': 1},
                'monthly_avg': {'$avg': '$quantity'}
            }},
            {'$project': {
                'material_code': '$_id.material_code',
                'material_name': '$_id.material_name',
                'unit': '$_id.unit',
                'material_group': '$_id.material_group',
                'plant': '$_id.plant',
                'total_consumption': 1,
                'month_count': 1,
                'monthly_avg': 1
            }}
        ]
        
        results = list(db.consumption_summary.aggregate(pipeline))
        print(f"Found {len(results)} monthly average records for plants: {plants_list}")
        
        avg_map = {}
        for r in results:
            material_code_key = str(r['material_code'])
            plant_key = str(r.get('plant', 'unknown'))
            
            if material_code_key not in avg_map:
                avg_map[material_code_key] = {}
            
            avg_map[material_code_key][plant_key] = {
                'monthly_avg': round(r['monthly_avg'], 2),
                'total_consumption': round(r['total_consumption'], 2),
                'month_count': r['month_count'],
                'material_name': r['material_name'] or str(r['material_code']),
                'unit': r['unit'] or 'Units',
                'material_group': r['material_group'] or 'Uncategorized',
                'plant': plant_key
            }
        
        # Fallback if no results
        if not results and plants_list:
            print("No results for specific plants, trying zone-level fallback")
            fallback_query = {'period_type': 'monthly'}
            if material_group and material_group != 'all':
                fallback_query['material_group'] = material_group
            if material_code and material_code != 'all':
                fallback_query['material_code'] = material_code
            
            fallback_pipeline = [
                {'$match': fallback_query},
                {'$group': {
                    '_id': {
                        'material_code': '$material_code',
                        'material_name': '$material_name',
                        'unit': '$unit',
                        'material_group': '$material_group'
                    },
                    'total_consumption': {'$sum': '$quantity'},
                    'month_count': {'$sum': 1},
                    'monthly_avg': {'$avg': '$quantity'}
                }},
                {'$project': {
                    'material_code': '$_id.material_code',
                    'material_name': '$_id.material_name',
                    'unit': '$_id.unit',
                    'material_group': '$_id.material_group',
                    'total_consumption': 1,
                    'month_count': 1,
                    'monthly_avg': 1
                }}
            ]
            
            fallback_results = list(db.consumption_summary.aggregate(fallback_pipeline))
            
            for r in fallback_results:
                material_code_key = str(r['material_code'])
                if material_code_key not in avg_map:
                    avg_map[material_code_key] = {}
                avg_map[material_code_key]['all'] = {
                    'monthly_avg': round(r['monthly_avg'], 2),
                    'total_consumption': round(r['total_consumption'], 2),
                    'month_count': r['month_count'],
                    'material_name': r['material_name'] or str(r['material_code']),
                    'unit': r['unit'] or 'Units',
                    'material_group': r['material_group'] or 'Uncategorized',
                    'plant': 'all'
                }
        
        print(f"Returning averages for {len(avg_map)} materials")
        return safe_json_response(avg_map)
        
    except Exception as e:
        print(f"Error in get_monthly_average_by_plant: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response({})


# ==================== MATERIAL IN TRANSIT ENDPOINTS ====================

@api_bp.route('/api/inventory/material-in-transit')
@login_required
def get_material_in_transit():
    """
    Get all materials currently in transit (DI and STN)
    Supports filtering with flexible plant code matching
    """
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        # Get filter parameters
        doc_type = request.args.get('doc_type', 'all')
        from_plant = request.args.get('from_plant', 'all')
        to_plant = request.args.get('to_plant', 'all')
        material_group = request.args.get('material_group', 'all')
        material_code = request.args.get('material_code', 'all')
        
        # Build query
        query = {}
        
        if doc_type and doc_type != 'all':
            query['document_type'] = doc_type
        
        # FIXED: Handle plant filtering with regex for STN
        if from_plant and from_plant != 'all' and from_plant != 'Vendor':
            # Use regex to find plant code anywhere in the string
            query['from_plant'] = {'$regex': from_plant, '$options': 'i'}
        
        if to_plant and to_plant != 'all':
            query['to_plant'] = {'$regex': to_plant, '$options': 'i'}
        
        if material_group and material_group != 'all':
            query['material_group'] = material_group
        
        if material_code and material_code != 'all':
            query['material_code'] = material_code
        
        print(f"Material In Transit Query: {query}")
        
        # Get transit items
        transit_items = list(db.material_in_transit.find(query, {'_id': 0}))
        
        # Add division names for plants
        for item in transit_items:
            # Get division for from_plant
            if item.get('from_plant'):
                from_division = db.storage_locations.find_one(
                    {'plant': item.get('from_plant')}, 
                    {'division': 1}
                )
                item['from_division'] = from_division.get('division', item.get('from_plant')) if from_division else item.get('from_plant')
            
            # Get division for to_plant
            if item.get('to_plant'):
                to_division = db.storage_locations.find_one(
                    {'plant': item.get('to_plant')}, 
                    {'division': 1}
                )
                item['to_division'] = to_division.get('division', item.get('to_plant')) if to_division else item.get('to_plant')
        
        print(f"Returning {len(transit_items)} transit items")
        return safe_json_response(transit_items)
        
    except Exception as e:
        print(f"Error in get_material_in_transit: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response([])


@api_bp.route('/api/inventory/transit-summary')
@login_required
def get_transit_summary():
    """
    Get summary statistics for material in transit
    """
    try:
        db = get_db()
        if db is None:
            return safe_json_response({
                'total_items': 0, 
                'total_quantity': 0, 
                'by_doc_type': {}, 
                'delayed_count': 0
            })
        
        # Get all in-transit items
        transit_items = list(db.material_in_transit.find({}))
        
        total_items = len(transit_items)
        total_quantity = sum(item.get('quantity', 0) for item in transit_items)
        
        # Group by document type
        by_doc_type = {}
        for item in transit_items:
            doc_type = item.get('document_type', 'Unknown')
            by_doc_type[doc_type] = by_doc_type.get(doc_type, 0) + 1
        
        # Count delayed shipments (>30 days based on document date)
        thirty_days_ago = datetime.now() - timedelta(days=30)
        delayed_count = 0
        
        for item in transit_items:
            doc_date = item.get('document_date')
            if doc_date:
                try:
                    if isinstance(doc_date, str):
                        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                            try:
                                doc_date = datetime.strptime(doc_date, fmt)
                                break
                            except:
                                continue
                    if isinstance(doc_date, datetime) and doc_date < thirty_days_ago:
                        delayed_count += 1
                except:
                    pass
        
        return safe_json_response({
            'total_items': total_items,
            'total_quantity': round(total_quantity, 2),
            'by_doc_type': by_doc_type,
            'delayed_count': delayed_count
        })
        
    except Exception as e:
        print(f"Error in get_transit_summary: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response({
            'total_items': 0, 
            'total_quantity': 0, 
            'by_doc_type': {}, 
            'delayed_count': 0
        })


# ==================== INVENTORY DASHBOARD ENDPOINTS ====================

@api_bp.route('/api/inventory/current-stock')
@login_required
def get_current_stock():
    """Get current stock levels from current_stock collection"""
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        stock = list(db.current_stock.find({}, {'_id': 0}))
        
        # Clean plant codes
        for item in stock:
            if 'plant' in item:
                item['plant'] = str(item['plant']).replace('.0', '')
        
        # If no data, return sample data for testing
        if not stock:
            sample_stock = [
                {'material_code': '102010611', 'material_name': 'M.S. CHANNEL 75 X 40MM', 'material_description': 'M.S. CHANNEL 75 X 40MM', 'material_group': 'MSCHANNEL', 'plant': '3400', 'current_stock': 203.341, 'unit': 'MT'},
                {'material_code': '102010611', 'material_name': 'M.S. CHANNEL 75 X 40MM', 'material_description': 'M.S. CHANNEL 75 X 40MM', 'material_group': 'MSCHANNEL', 'plant': '3412', 'current_stock': 14.805, 'unit': 'MT'},
                {'material_code': '101011011', 'material_name': 'M.S ANGLE 50 X 50 X 6MM', 'material_description': 'M.S ANGLE 50 X 50 X 6MM', 'material_group': 'MSANGLE', 'plant': '3400', 'current_stock': 221.527, 'unit': 'MT'},
                {'material_code': '101011011', 'material_name': 'M.S ANGLE 50 X 50 X 6MM', 'material_description': 'M.S ANGLE 50 X 50 X 6MM', 'material_group': 'MSANGLE', 'plant': '3412', 'current_stock': 18.323, 'unit': 'MT'},
            ]
            return safe_json_response(sample_stock)
        
        print(f"Returning {len(stock)} stock records")
        return safe_json_response(stock)
    except Exception as e:
        print(f"Error in get_current_stock: {e}")
        return safe_json_response([])


@api_bp.route('/api/inventory/critical-items')
@login_required
def get_critical_items():
    """Get items below min stock level"""
    try:
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        pipeline = [
            {'$lookup': {
                'from': 'material_master',
                'localField': 'material_code',
                'foreignField': 'Material_Code',
                'as': 'material'
            }},
            {'$addFields': {
                'min_stock': {'$ifNull': [{'$arrayElemAt': ['$material.calculated_min_stock', 0]}, 0]}
            }},
            {'$match': {
                '$expr': {'$lt': ['$current_stock', '$min_stock']}
            }}
        ]
        
        critical = list(db.current_stock.aggregate(pipeline))
        for item in critical:
            item['_id'] = str(item['_id'])
        
        return safe_json_response(critical)
    except Exception as e:
        print(f"Error in get_critical_items: {e}")
        return safe_json_response([])


# ==================== LEGACY ENDPOINTS ====================

@api_bp.route('/api/dashboard/administrative', methods=['POST'])
@login_required
def get_dashboard_administrative():
    return get_admin_data()


@api_bp.route('/api/priority-works/overview')
@login_required
def get_priority_works_overview():
    return safe_json_response({
        'hvds': {'total': 8, 'completed': 3, 'in_progress': 5, 'percentage': '65%', 'budget': '₹8.2Cr'},
        'newSubstation': {'total': 3, 'capacity': '120 MVA', 'progress': '45%', 'budget': '₹6.5Cr', 'target': 'Dec 2026'},
        'ptr': {'total': 12, 'completed': 5, 'capacity': '85 MVA', 'progress': '42%', 'budget': '₹4.8Cr'},
        'new33kv': {'count': 2, 'length': '28 km', 'towers': 84, 'budget': '₹3.2Cr', 'start': 'Apr 2026'},
        'cond33kv': {'count': 8, 'length': '42 km', 'completed': 3, 'progress': '38%', 'budget': '₹2.8Cr'},
        'new11kv': {'count': 5, 'length': '15 km', 'poles': 225, 'budget': '₹1.8Cr', 'start': 'May 2026'},
        'cond11kv': {'count': 12, 'length': '38 km', 'completed': 12, 'progress': '100%', 'budget': '₹1.2Cr'}
    })