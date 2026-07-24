"""
API Routes - Enhanced with Caching for Fast Response
Using consumption_summary collection for consumption data
Last Updated: July 24, 2026
- Added In-Memory Caching for 10x faster responses
- Added Cache Management endpoints
- Optimized MongoDB queries with indexes
- Added Allotment Tracker API endpoints
- Added Division-Region mapping endpoint with proper name mapping
- Added Pending Allotment endpoints for User→Manager workflow
- Added Finalize Allotment endpoint for Manager
- ★ NEW: Added Unit Weight (MT) to materials endpoint for Transport Optimization
- ★ NEW: Added weight calculation endpoints
- ★ NEW: Added bulk weight update endpoint
- ★ NEW: Added /api/last-updated endpoint for data sync timestamp
- ★ FIXED: Proper NaN handling in safe_json_response
"""

from flask import Blueprint, jsonify, session, request
from app.utils.decorators import login_required
from app.models.mongo_utils import get_db
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import re
import hashlib
import json
import uuid
import math

api_bp = Blueprint('api', __name__)

# ================================================================
# IN-MEMORY CACHE SYSTEM
# ================================================================

class DataCache:
    """Simple in-memory cache for MongoDB data"""
    
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = {}
        self.default_ttl = 300  # 5 minutes default
        self._hit_count = 0
        self._miss_count = 0
    
    def get(self, key):
        """Get cached data if not expired"""
        if key in self._cache and key in self._cache_time:
            ttl = self._cache_ttl.get(key, self.default_ttl)
            if datetime.now() - self._cache_time[key] < timedelta(seconds=ttl):
                self._hit_count += 1
                print(f"📦 Cache HIT: {key}")
                return self._cache[key]
            else:
                print(f"⏰ Cache EXPIRED: {key}")
        else:
            print(f"❌ Cache MISS: {key}")
        self._miss_count += 1
        return None
    
    def set(self, key, data, ttl=None):
        """Set cached data with optional TTL"""
        self._cache[key] = data
        self._cache_time[key] = datetime.now()
        self._cache_ttl[key] = ttl or self.default_ttl
        print(f"💾 Cache SET: {key} (TTL: {self._cache_ttl[key]}s)")
    
    def clear(self, key=None):
        """Clear specific cache or all cache"""
        if key:
            self._cache.pop(key, None)
            self._cache_time.pop(key, None)
            self._cache_ttl.pop(key, None)
            print(f"🗑️ Cache CLEAR: {key}")
        else:
            self._cache.clear()
            self._cache_time.clear()
            self._cache_ttl.clear()
            self._hit_count = 0
            self._miss_count = 0
            print("🗑️ Cache CLEAR: All")
    
    def get_stats(self):
        """Get cache statistics"""
        total_requests = self._hit_count + self._miss_count
        hit_rate = round((self._hit_count / total_requests * 100) if total_requests > 0 else 0, 2)
        return {
            'total_items': len(self._cache),
            'keys': list(self._cache.keys()),
            'size_kb': round(sum(len(str(v)) for v in self._cache.values()) / 1024, 2),
            'hit_count': self._hit_count,
            'miss_count': self._miss_count,
            'hit_rate': f"{hit_rate}%"
        }

# Global cache instance
_cache = DataCache()

def get_cache_key(*args, **kwargs):
    """Generate cache key from arguments"""
    key_str = str(args) + str(sorted(kwargs.items()))
    return hashlib.md5(key_str.encode()).hexdigest()

# ================================================================
# ★ FIXED: HELPER FUNCTIONS WITH NaN HANDLING
# ================================================================

def clean_nan(obj):
    """Recursively replace NaN, Infinity with None for valid JSON"""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, str):
        # Check if string is 'nan' or 'NaN'
        if obj.lower() == 'nan':
            return None
        return obj
    else:
        return obj

def safe_json_response(data, status=200):
    """Convert data to JSON safely, replacing NaN with None"""
    try:
        cleaned_data = clean_nan(data)
        return jsonify(cleaned_data), status
    except Exception as e:
        print(f"Error in safe_json_response: {e}")
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
    match = re.search(r'(\d{4})', val_str)
    if match:
        return match.group(1)
    match = re.search(r'(\d+)', val_str)
    if match:
        return match.group(1)
    return ''

def generate_allotment_id():
    """Generate a unique allotment ID"""
    return f"ALL-{datetime.now().strftime('%Y%m')}-{str(uuid.uuid4())[:8].upper()}"

def get_division_name(plant_code):
    """Get division name from plant code"""
    plant_to_division = {
        '3400': 'Siliguri Zonal Store', '3412': 'Siliguri Town Division', '3413': 'Kurseong Division',
        '3414': 'Darjeeling Division', '3415': 'Sub-Urban Division', '3422': 'Jalpaiguri Division',
        '3427': 'Mal Division', '3444': 'Coochbehar Division', '3445': 'Mathabhanga Division',
        '3446': 'Dinhata Division', '3453': 'Alipurduar Division', '3471': 'Kalimpong Division',
        '3600': 'Central Store'
    }
    return plant_to_division.get(str(plant_code), 'Unknown Division')

# ================================================================
# DIVISION-REGION MAPPING HELPERS
# ================================================================

def get_full_division_name(short_name):
    """Map short division names (from DB) to full names (used in frontend)"""
    if not short_name:
        return ''
    
    if short_name.endswith('Division'):
        return short_name
    
    mapping = {
        'Siliguri Town': 'Siliguri Town Division',
        'Kurseong': 'Kurseong Division',
        'Darjeeling': 'Darjeeling Division',
        'Sub-Urban': 'Sub-Urban Division',
        'Kalimpong': 'Kalimpong Division',
        'Jalpaiguri': 'Jalpaiguri Division',
        'Mal': 'Mal Division',
        'Coochbehar': 'Coochbehar Division',
        'Mathabhanga': 'Mathabhanga Division',
        'Dinhata': 'Dinhata Division',
        'Alipurduar': 'Alipurduar Division'
    }
    
    return mapping.get(short_name, short_name + ' Division')

def clean_region_name(region_id, region_name=None):
    """Clean region name from region_id or region_name"""
    if region_name and not region_name.startswith('reg_'):
        if region_name.endswith('Region'):
            return region_name
        return region_name + ' Region'
    
    if region_id and region_id.startswith('reg_'):
        name_part = region_id.replace('reg_', '')
        return name_part.capitalize() + ' Region'
    
    return region_name or 'Unknown Region'

def get_fallback_division_region_mapping():
    """Fallback hardcoded mapping if MongoDB fetch fails"""
    return [
        {'division': 'Siliguri Town Division', 'region': 'Darjeeling Region', 'region_id': 'reg_darjeeling'},
        {'division': 'Kurseong Division', 'region': 'Darjeeling Region', 'region_id': 'reg_darjeeling'},
        {'division': 'Darjeeling Division', 'region': 'Darjeeling Region', 'region_id': 'reg_darjeeling'},
        {'division': 'Sub-Urban Division', 'region': 'Darjeeling Region', 'region_id': 'reg_darjeeling'},
        {'division': 'Kalimpong Division', 'region': 'Kalimpong Region', 'region_id': 'reg_kalimpong'},
        {'division': 'Jalpaiguri Division', 'region': 'Jalpaiguri Region', 'region_id': 'reg_jalpaiguri'},
        {'division': 'Mal Division', 'region': 'Jalpaiguri Region', 'region_id': 'reg_jalpaiguri'},
        {'division': 'Coochbehar Division', 'region': 'Coochbehar Region', 'region_id': 'reg_coochbehar'},
        {'division': 'Mathabhanga Division', 'region': 'Coochbehar Region', 'region_id': 'reg_coochbehar'},
        {'division': 'Dinhata Division', 'region': 'Coochbehar Region', 'region_id': 'reg_coochbehar'},
        {'division': 'Alipurduar Division', 'region': 'Alipurduar Region', 'region_id': 'reg_alipurduar'}
    ]

# ================================================================
# DIVISION-REGION MAPPING ENDPOINT
# ================================================================

@api_bp.route('/api/divisions/with-regions')
@login_required
def get_divisions_with_regions():
    """Get divisions with their region mapping from MongoDB - CACHED for 1 hour"""
    try:
        cache_key = 'divisions_with_regions'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            fallback = get_fallback_division_region_mapping()
            _cache.set(cache_key, fallback, ttl=3600)
            return safe_json_response(fallback)
        
        divisions = list(db.divisions.find({}, {'_id': 0, 'name': 1, 'region_id': 1}))
        regions = list(db.regions.find({}, {'_id': 0, '_id': 1, 'name': 1}))
        
        region_map = {}
        for region in regions:
            region_id = region.get('_id')
            region_name = region.get('name')
            if region_id:
                region_map[region_id] = clean_region_name(region_id, region_name)
        
        result = []
        for div in divisions:
            div_name = div.get('name')
            region_id = div.get('region_id')
            
            if not div_name:
                continue
            
            full_div_name = get_full_division_name(div_name)
            
            if region_id and region_id in region_map:
                region_name = region_map[region_id]
            else:
                region_name = clean_region_name(region_id, None)
            
            result.append({
                'division': full_div_name,
                'region': region_name,
                'region_id': region_id
            })
        
        if not result:
            result = get_fallback_division_region_mapping()
        
        _cache.set(cache_key, result, ttl=3600)
        print(f"✅ Division-Region mapping loaded: {len(result)} divisions")
        return safe_json_response(result)
        
    except Exception as e:
        print(f"Error in get_divisions_with_regions: {e}")
        import traceback
        traceback.print_exc()
        fallback = get_fallback_division_region_mapping()
        return safe_json_response(fallback)

# ================================================================
# CACHE MANAGEMENT ENDPOINTS
# ================================================================

@api_bp.route('/api/cache/clear', methods=['POST'])
@login_required
def clear_cache():
    """Clear all cache - Admin only"""
    try:
        user = session.get('user', {})
        if user.get('role') != 'admin':
            return safe_json_response({'error': 'Admin access required'}, 403)
        
        _cache.clear()
        return safe_json_response({'message': 'Cache cleared successfully', 'status': 'success'})
    except Exception as e:
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/cache/status')
@login_required
def cache_status():
    """Get cache status - Admin only"""
    try:
        user = session.get('user', {})
        if user.get('role') != 'admin':
            return safe_json_response({'error': 'Admin access required'}, 403)
        
        return safe_json_response(_cache.get_stats())
    except Exception as e:
        return safe_json_response({'error': str(e)}, 500)

# ================================================================
# BULK ALLOTMENT ENDPOINTS
# ================================================================

@api_bp.route('/api/allotments/bulk', methods=['POST'])
@login_required
def save_bulk_allotment():
    """Save bulk allotment to database - Manager only"""
    try:
        user = session.get('user', {})
        
        data = request.json or {}
        
        required_fields = ['allotment_no', 'divisions', 'allotments']
        for field in required_fields:
            if not data.get(field):
                return safe_json_response({'error': f'Missing required field: {field}'}, 400)
        
        data['created_by'] = user.get('username', 'Unknown')
        data['created_by_name'] = user.get('name', 'Unknown')
        data['user_role'] = user.get('role', 'user')
        data['created_date'] = datetime.now().isoformat()
        data['updated_date'] = datetime.now().isoformat()
        
        if data.get('transport_weight') and data.get('allotments'):
            total_weight = calculate_total_allotment_weight(data['allotments'])
            data['total_weight_kg'] = total_weight
            data['utilization_percent'] = round((total_weight / data['transport_weight']) * 100, 1) if data['transport_weight'] > 0 else 0
        
        db = get_db()
        if db is None:
            return safe_json_response({'error': 'Database not connected'}, 500)
        
        collections = db.list_collection_names()
        if 'allotments' not in collections:
            db.create_collection('allotments')
        
        result = db.allotments.insert_one(data)
        
        _cache.clear('allotments_data')
        _cache.clear('allotment_summary')
        _cache.clear('allotment_history')
        
        return safe_json_response({
            'success': True, 
            'id': str(result.inserted_id),
            'allotment_no': data.get('allotment_no'),
            'total_weight': data.get('total_weight_kg', 0),
            'message': f'Allotment {data.get("allotment_no")} saved successfully'
        }, 201)
        
    except Exception as e:
        print(f"Error in save_bulk_allotment: {e}")
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/allotments/bulk/history')
@login_required
def get_allotment_history():
    """Get allotment history - CACHED for 2 minutes"""
    try:
        cache_key = 'allotment_history'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        collections = db.list_collection_names()
        if 'allotments' not in collections:
            return safe_json_response([])
        
        history = list(db.allotments.find(
            {}, 
            {'_id': 0}
        ).sort('created_date', -1).limit(50))
        
        for item in history:
            total = 0
            if item.get('allotments'):
                for allot in item['allotments']:
                    total += allot.get('allotted_qty', 0)
            item['total_allotted'] = total
        
        _cache.set(cache_key, history, ttl=120)
        return safe_json_response(history)
        
    except Exception as e:
        print(f"Error in get_allotment_history: {e}")
        return safe_json_response([])

# ================================================================
# PENDING ALLOTMENT ENDPOINTS
# ================================================================

@api_bp.route('/api/allotments/bulk/pending')
@login_required
def get_pending_allotments():
    """Get all pending allotments - Manager only"""
    try:
        user = session.get('user', {})
        if user.get('role') not in ['admin', 'manager']:
            return safe_json_response({'error': 'Only Manager can view pending allotments'}, 403)
        
        cache_key = 'pending_allotments'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        collections = db.list_collection_names()
        if 'allotments' not in collections:
            return safe_json_response([])
        
        pending = list(db.allotments.find(
            {'status': 'Pending'},
            {'_id': 1, 'allotment_no': 1, 'memo_no': 1, 'allotment_date': 1, 
             'divisions': 1, 'materials': 1, 'allotments': 1, 'sto_numbers': 1,
             'created_by': 1, 'created_by_name': 1, 'created_date': 1,
             'transport_weight': 1, 'total_weight_kg': 1, 'utilization_percent': 1}
        ).sort('created_date', -1))
        
        for item in pending:
            item['_id'] = str(item['_id'])
        
        _cache.set(cache_key, pending, ttl=60)
        return safe_json_response(pending)
        
    except Exception as e:
        print(f"Error in get_pending_allotments: {e}")
        return safe_json_response([])

@api_bp.route('/api/allotments/bulk/pending/<allotment_id>')
@login_required
def get_pending_allotment(allotment_id):
    """Get a specific pending allotment by ID - Manager only"""
    try:
        user = session.get('user', {})
        if user.get('role') not in ['admin', 'manager']:
            return safe_json_response({'error': 'Only Manager can view pending allotments'}, 403)
        
        from bson import ObjectId
        db = get_db()
        if db is None:
            return safe_json_response({})
        
        collections = db.list_collection_names()
        if 'allotments' not in collections:
            return safe_json_response({})
        
        try:
            obj_id = ObjectId(allotment_id)
            allotment = db.allotments.find_one(
                {'_id': obj_id, 'status': 'Pending'}
            )
            if allotment:
                allotment['_id'] = str(allotment['_id'])
                return safe_json_response(allotment)
        except:
            allotment = db.allotments.find_one(
                {'_id': allotment_id, 'status': 'Pending'}
            )
            if allotment:
                allotment['_id'] = str(allotment['_id'])
                return safe_json_response(allotment)
        
        return safe_json_response({})
        
    except Exception as e:
        print(f"Error in get_pending_allotment: {e}")
        return safe_json_response({})

@api_bp.route('/api/allotments/<allotment_id>/finalize', methods=['PUT'])
@login_required
def finalize_allotment(allotment_id):
    """Finalize a pending allotment - Manager only"""
    try:
        user = session.get('user', {})
        if user.get('role') not in ['admin', 'manager']:
            return safe_json_response({'error': 'Only Manager can finalize allotments'}, 403)
        
        data = request.json or {}
        
        from bson import ObjectId
        db = get_db()
        if db is None:
            return safe_json_response({'error': 'Database not connected'}, 500)
        
        collections = db.list_collection_names()
        if 'allotments' not in collections:
            return safe_json_response({'error': 'Allotments collection not found'}, 404)
        
        update_data = {
            'status': 'Completed',
            'finalized_by': user.get('username', 'Unknown'),
            'finalized_by_name': user.get('name', 'Unknown'),
            'finalized_date': datetime.now().isoformat(),
            'updated_date': datetime.now().isoformat()
        }
        
        if data.get('allotments'):
            update_data['allotments'] = data['allotments']
        if data.get('sto_numbers'):
            update_data['sto_numbers'] = data['sto_numbers']
        if data.get('memo_no'):
            update_data['memo_no'] = data['memo_no']
        if data.get('allotment_no'):
            update_data['allotment_no'] = data['allotment_no']
        if data.get('allotment_date'):
            update_data['allotment_date'] = data['allotment_date']
        if data.get('divisions'):
            update_data['divisions'] = data['divisions']
        if data.get('materials'):
            update_data['materials'] = data['materials']
        if data.get('transport_weight'):
            update_data['transport_weight'] = data['transport_weight']
        if data.get('total_weight_kg'):
            update_data['total_weight_kg'] = data['total_weight_kg']
        if data.get('utilization_percent'):
            update_data['utilization_percent'] = data['utilization_percent']
        
        try:
            obj_id = ObjectId(allotment_id)
            result = db.allotments.update_one(
                {'_id': obj_id, 'status': 'Pending'},
                {'$set': update_data}
            )
        except:
            result = db.allotments.update_one(
                {'_id': allotment_id, 'status': 'Pending'},
                {'$set': update_data}
            )
        
        if result.modified_count > 0:
            _cache.clear('pending_allotments')
            _cache.clear('allotment_history')
            _cache.clear('allotments_data')
            return safe_json_response({'success': True, 'message': 'Allotment finalized successfully'})
        
        return safe_json_response({'error': 'Allotment not found or already finalized'}, 404)
        
    except Exception as e:
        print(f"Error in finalize_allotment: {e}")
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/allotments/bulk/save-pending', methods=['POST'])
@login_required
def save_pending_allotment():
    """Save allotment as PENDING - User sends to Manager"""
    try:
        user = session.get('user', {})
        data = request.json or {}
        
        required_fields = ['allotment_no', 'divisions', 'allotments']
        for field in required_fields:
            if not data.get(field):
                return safe_json_response({'error': f'Missing required field: {field}'}, 400)
        
        data['created_by'] = user.get('username', 'Unknown')
        data['created_by_name'] = user.get('name', 'Unknown')
        data['user_role'] = user.get('role', 'user')
        data['status'] = 'Pending'
        data['created_date'] = datetime.now().isoformat()
        data['updated_date'] = datetime.now().isoformat()
        
        if data.get('transport_weight') and data.get('allotments'):
            total_weight = calculate_total_allotment_weight(data['allotments'])
            data['total_weight_kg'] = total_weight
            data['utilization_percent'] = round((total_weight / data['transport_weight']) * 100, 1) if data['transport_weight'] > 0 else 0
        
        db = get_db()
        if db is None:
            return safe_json_response({'error': 'Database not connected'}, 500)
        
        collections = db.list_collection_names()
        if 'allotments' not in collections:
            db.create_collection('allotments')
        
        result = db.allotments.insert_one(data)
        
        _cache.clear('pending_allotments')
        _cache.clear('allotment_history')
        _cache.clear('allotments_data')
        
        return safe_json_response({
            'success': True, 
            'id': str(result.inserted_id),
            'allotment_no': data.get('allotment_no'),
            'status': 'Pending',
            'total_weight': data.get('total_weight_kg', 0),
            'message': f'Allotment {data.get("allotment_no")} saved as PENDING successfully'
        }, 201)
        
    except Exception as e:
        print(f"Error in save_pending_allotment: {e}")
        return safe_json_response({'error': str(e)}, 500)

# ================================================================
# ALLOTMENT TRACKER API ENDPOINTS
# ================================================================

@api_bp.route('/api/allotments')
@login_required
def get_allotments():
    """Get all allotment requests - CACHED for 2 minutes"""
    try:
        cache_key = 'allotments_data'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        try:
            collections = db.list_collection_names()
            if 'allotments' in collections:
                allotments = list(db.allotments.find({}, {'_id': 0}).sort('created_date', -1))
                if allotments:
                    _cache.set(cache_key, allotments, ttl=120)
                    return safe_json_response(allotments)
        except:
            pass
        
        sample_allotments = generate_sample_allotments()
        _cache.set(cache_key, sample_allotments, ttl=120)
        return safe_json_response(sample_allotments)
        
    except Exception as e:
        print(f"Error in get_allotments: {e}")
        return safe_json_response([])

@api_bp.route('/api/allotments', methods=['POST'])
@login_required
def create_allotment():
    """Create a new allotment request"""
    try:
        user = session.get('user', {})
        data = request.json or {}
        
        required_fields = ['material_code', 'division', 'requested_qty']
        for field in required_fields:
            if not data.get(field):
                return safe_json_response({'error': f'Missing required field: {field}'}, 400)
        
        allotment = {
            'id': generate_allotment_id(),
            'material_code': data.get('material_code'),
            'material_name': data.get('material_name', 'Unknown Material'),
            'material_group': data.get('material_group', 'Uncategorized'),
            'division': data.get('division'),
            'requested_qty': float(data.get('requested_qty', 0)),
            'approved_qty': 0,
            'unit': data.get('unit', 'Nos'),
            'request_date': datetime.now().isoformat(),
            'status': 'Pending',
            'requested_by': user.get('username', 'Unknown'),
            'requested_by_name': user.get('name', 'Unknown'),
            'remarks': data.get('remarks', ''),
            'priority': data.get('priority', 'Normal')
        }
        
        db = get_db()
        if db is not None:
            try:
                collections = db.list_collection_names()
                if 'allotments' not in collections:
                    db.create_collection('allotments')
                
                db.allotments.insert_one(allotment)
                _cache.clear('allotments_data')
                return safe_json_response({'success': True, 'allotment': allotment}, 201)
            except Exception as e:
                print(f"Database error: {e}")
        
        if not hasattr(api_bp, 'memory_allotments'):
            api_bp.memory_allotments = []
        api_bp.memory_allotments.append(allotment)
        _cache.clear('allotments_data')
        
        return safe_json_response({'success': True, 'allotment': allotment}, 201)
        
    except Exception as e:
        print(f"Error in create_allotment: {e}")
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/allotments/<allotment_id>/approve', methods=['POST'])
@login_required
def approve_allotment(allotment_id):
    """Approve an allotment request"""
    try:
        user = session.get('user', {})
        if user.get('role') not in ['admin', 'manager']:
            return safe_json_response({'error': 'Only admin or manager can approve allotments'}, 403)
        
        data = request.json or {}
        approved_qty = data.get('approved_qty', 0)
        
        db = get_db()
        if db is not None:
            try:
                result = db.allotments.update_one(
                    {'id': allotment_id},
                    {'$set': {
                        'status': 'Approved' if approved_qty > 0 else 'Rejected',
                        'approved_qty': approved_qty,
                        'approved_by': user.get('username', 'Unknown'),
                        'approved_by_name': user.get('name', 'Unknown'),
                        'approved_date': datetime.now().isoformat(),
                        'remarks': data.get('remarks', '')
                    }}
                )
                if result.modified_count > 0:
                    _cache.clear('allotments_data')
                    return safe_json_response({'success': True, 'message': f'Allotment {allotment_id} approved'})
            except Exception as e:
                print(f"Database error: {e}")
        
        if hasattr(api_bp, 'memory_allotments'):
            for item in api_bp.memory_allotments:
                if item['id'] == allotment_id and item['status'] == 'Pending':
                    item['status'] = 'Approved' if approved_qty > 0 else 'Rejected'
                    item['approved_qty'] = approved_qty
                    item['approved_by'] = user.get('username', 'Unknown')
                    item['approved_date'] = datetime.now().isoformat()
                    _cache.clear('allotments_data')
                    return safe_json_response({'success': True, 'message': f'Allotment {allotment_id} approved'})
        
        return safe_json_response({'error': 'Allotment not found or already processed'}, 404)
        
    except Exception as e:
        print(f"Error in approve_allotment: {e}")
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/allotments/<allotment_id>/reject', methods=['POST'])
@login_required
def reject_allotment(allotment_id):
    """Reject an allotment request"""
    try:
        user = session.get('user', {})
        if user.get('role') not in ['admin', 'manager']:
            return safe_json_response({'error': 'Only admin or manager can reject allotments'}, 403)
        
        data = request.json or {}
        
        db = get_db()
        if db is not None:
            try:
                result = db.allotments.update_one(
                    {'id': allotment_id},
                    {'$set': {
                        'status': 'Rejected',
                        'approved_qty': 0,
                        'rejected_by': user.get('username', 'Unknown'),
                        'rejected_by_name': user.get('name', 'Unknown'),
                        'rejected_date': datetime.now().isoformat(),
                        'remarks': data.get('remarks', 'Request rejected')
                    }}
                )
                if result.modified_count > 0:
                    _cache.clear('allotments_data')
                    return safe_json_response({'success': True, 'message': f'Allotment {allotment_id} rejected'})
            except Exception as e:
                print(f"Database error: {e}")
        
        if hasattr(api_bp, 'memory_allotments'):
            for item in api_bp.memory_allotments:
                if item['id'] == allotment_id and item['status'] == 'Pending':
                    item['status'] = 'Rejected'
                    item['approved_qty'] = 0
                    item['rejected_by'] = user.get('username', 'Unknown')
                    item['rejected_date'] = datetime.now().isoformat()
                    _cache.clear('allotments_data')
                    return safe_json_response({'success': True, 'message': f'Allotment {allotment_id} rejected'})
        
        return safe_json_response({'error': 'Allotment not found or already processed'}, 404)
        
    except Exception as e:
        print(f"Error in reject_allotment: {e}")
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/allotments/summary')
@login_required
def get_allotment_summary():
    """Get allotment summary - CACHED for 2 minutes"""
    try:
        cache_key = 'allotment_summary'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        allotments = []
        db = get_db()
        if db is not None:
            try:
                collections = db.list_collection_names()
                if 'allotments' in collections:
                    allotments = list(db.allotments.find({}, {'_id': 0}))
            except:
                pass
        
        if not allotments and hasattr(api_bp, 'memory_allotments'):
            allotments = api_bp.memory_allotments
        
        if not allotments:
            allotments = generate_sample_allotments()
        
        total = len(allotments)
        pending = len([a for a in allotments if a.get('status') == 'Pending'])
        approved = len([a for a in allotments if a.get('status') == 'Approved'])
        rejected = len([a for a in allotments if a.get('status') == 'Rejected'])
        partially = len([a for a in allotments if a.get('status') == 'Partially Approved'])
        
        now = datetime.now()
        monthly = len([a for a in allotments if a.get('request_date') and 
                      datetime.fromisoformat(a['request_date']).month == now.month and
                      datetime.fromisoformat(a['request_date']).year == now.year])
        
        completion_rate = round((approved / total * 100) if total > 0 else 0, 1)
        
        response = {
            'total': total,
            'pending': pending,
            'approved': approved,
            'rejected': rejected,
            'partially_approved': partially,
            'monthly': monthly,
            'completion_rate': completion_rate
        }
        
        _cache.set(cache_key, response, ttl=120)
        return safe_json_response(response)
        
    except Exception as e:
        print(f"Error in get_allotment_summary: {e}")
        return safe_json_response({
            'total': 0, 'pending': 0, 'approved': 0, 'rejected': 0, 
            'partially_approved': 0, 'monthly': 0, 'completion_rate': 0
        })

# ================================================================
# ★ NEW: TRANSPORT WEIGHT ENDPOINTS
# ================================================================

def calculate_total_allotment_weight(allotments):
    """Calculate total weight of allotment items in KG"""
    db = get_db()
    if db is None:
        return 0
    
    total_weight_kg = 0
    
    material_codes = list(set([a.get('material_code') for a in allotments if a.get('material_code')]))
    if not material_codes:
        return 0
    
    material_weights = {}
    try:
        weight_records = db.material_master.find(
            {'Material_Code': {'$in': material_codes}},
            {'Material_Code': 1, 'Unit Weight (MT)': 1}
        )
        for record in weight_records:
            code = record.get('Material_Code')
            weight_mt = record.get('Unit Weight (MT)', 0)
            if weight_mt and weight_mt > 0:
                material_weights[code] = weight_mt * 1000
            else:
                material_weights[code] = 0
    except Exception as e:
        print(f"Error fetching weights: {e}")
        return 0
    
    for allotment in allotments:
        code = allotment.get('material_code')
        qty = float(allotment.get('allotted_qty', 0))
        weight_per_unit_kg = material_weights.get(code, 0)
        total_weight_kg += qty * weight_per_unit_kg
    
    return round(total_weight_kg, 2)

@api_bp.route('/api/weight/calculate', methods=['POST'])
@login_required
def calculate_weight():
    """Calculate total weight for given allotment items"""
    try:
        data = request.json or {}
        allotments = data.get('allotments', [])
        
        if not allotments:
            return safe_json_response({'total_weight_kg': 0, 'items': []})
        
        db = get_db()
        if db is None:
            return safe_json_response({'error': 'Database not connected'}, 500)
        
        material_codes = list(set([a.get('material_code') for a in allotments if a.get('material_code')]))
        if not material_codes:
            return safe_json_response({'total_weight_kg': 0, 'items': []})
        
        material_weights = {}
        material_names = {}
        try:
            records = db.material_master.find(
                {'Material_Code': {'$in': material_codes}},
                {'Material_Code': 1, 'Unit Weight (MT)': 1, 'Material Description': 1, 'Unit of Entry': 1}
            )
            for record in records:
                code = record.get('Material_Code')
                weight_mt = record.get('Unit Weight (MT)', 0)
                material_weights[code] = weight_mt * 1000 if weight_mt else 0
                material_names[code] = record.get('Material Description', code)
        except Exception as e:
            print(f"Error fetching weights: {e}")
        
        items_with_weight = []
        total_weight_kg = 0
        for allotment in allotments:
            code = allotment.get('material_code')
            qty = float(allotment.get('allotted_qty', 0))
            weight_per_unit_kg = material_weights.get(code, 0)
            item_weight = qty * weight_per_unit_kg
            total_weight_kg += item_weight
            
            items_with_weight.append({
                'material_code': code,
                'material_name': material_names.get(code, code),
                'unit': allotment.get('unit', 'NOS'),
                'allotted_qty': qty,
                'weight_per_unit_kg': weight_per_unit_kg,
                'total_weight_kg': round(item_weight, 2)
            })
        
        return safe_json_response({
            'total_weight_kg': round(total_weight_kg, 2),
            'items': items_with_weight
        })
        
    except Exception as e:
        print(f"Error in calculate_weight: {e}")
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/weight/material/<material_code>')
@login_required
def get_material_weight(material_code):
    """Get weight for a specific material"""
    try:
        if not material_code:
            return safe_json_response({'error': 'Material code required'}, 400)
        
        db = get_db()
        if db is None:
            return safe_json_response({'error': 'Database not connected'}, 500)
        
        record = db.material_master.find_one(
            {'Material_Code': material_code},
            {'Material_Code': 1, 'Unit Weight (MT)': 1, 'Material Description': 1, 'Unit of Entry': 1}
        )
        
        if record:
            weight_mt = record.get('Unit Weight (MT)', 0)
            return safe_json_response({
                'material_code': material_code,
                'material_name': record.get('Material Description', material_code),
                'unit': record.get('Unit of Entry', 'NOS'),
                'weight_per_unit_mt': weight_mt if weight_mt else 0,
                'weight_per_unit_kg': (weight_mt * 1000) if weight_mt else 0
            })
        else:
            return safe_json_response({'error': 'Material not found'}, 404)
        
    except Exception as e:
        print(f"Error in get_material_weight: {e}")
        return safe_json_response({'error': str(e)}, 500)

@api_bp.route('/api/weight/bulk', methods=['POST'])
@login_required
def get_bulk_material_weights():
    """Get weights for multiple materials"""
    try:
        data = request.json or {}
        material_codes = data.get('material_codes', [])
        
        if not material_codes:
            return safe_json_response({})
        
        db = get_db()
        if db is None:
            return safe_json_response({'error': 'Database not connected'}, 500)
        
        records = db.material_master.find(
            {'Material_Code': {'$in': material_codes}},
            {'Material_Code': 1, 'Unit Weight (MT)': 1, 'Material Description': 1, 'Unit of Entry': 1}
        )
        
        result = {}
        for record in records:
            code = record.get('Material_Code')
            weight_mt = record.get('Unit Weight (MT)', 0)
            result[code] = {
                'material_name': record.get('Material Description', code),
                'unit': record.get('Unit of Entry', 'NOS'),
                'weight_per_unit_mt': weight_mt if weight_mt else 0,
                'weight_per_unit_kg': (weight_mt * 1000) if weight_mt else 0
            }
        
        return safe_json_response(result)
        
    except Exception as e:
        print(f"Error in get_bulk_material_weights: {e}")
        return safe_json_response({'error': str(e)}, 500)

# ================================================================
# SAMPLE ALLOTMENT DATA GENERATOR
# ================================================================

def generate_sample_allotments():
    """Generate sample allotment data for testing"""
    divisions = [
        'Siliguri Town Division', 'Kurseong Division', 'Darjeeling Division', 
        'Sub-Urban Division', 'Kalimpong Division', 'Jalpaiguri Division', 
        'Mal Division', 'Coochbehar Division', 'Mathabhanga Division', 
        'Dinhata Division', 'Alipurduar Division'
    ]
    
    materials = [
        {'code': 'MAT-001', 'name': '11KV XLPE Cable 3x240 sqmm', 'group': 'Cables'},
        {'code': 'MAT-002', 'name': '33KV Isolator 200A', 'group': 'Switchgear'},
        {'code': 'MAT-003', 'name': 'Distribution Transformer 250KVA', 'group': 'Transformers'},
        {'code': 'MAT-004', 'name': 'LT PVC Cable 4x95 sqmm', 'group': 'Cables'},
        {'code': 'MAT-005', 'name': 'Lightning Arrester 10KA', 'group': 'Protection'},
        {'code': 'MAT-006', 'name': 'CT Metering 100/5A', 'group': 'Metering'},
        {'code': 'MAT-007', 'name': 'PTR 33/11KV 10MVA', 'group': 'Transformers'},
        {'code': 'MAT-008', 'name': 'AB Cable 4x16 sqmm', 'group': 'Cables'}
    ]
    
    statuses = ['Pending', 'Approved', 'Rejected', 'Partially Approved']
    
    sample_data = []
    for i in range(1, 26):
        material = materials[i % len(materials)]
        division = divisions[i % len(divisions)]
        status = statuses[i % len(statuses)]
        requested_qty = (i * 7) + 10
        approved_qty = requested_qty if status == 'Approved' else (
            int(requested_qty * 0.6) if status == 'Partially Approved' else 0
        )
        
        date = datetime.now() - timedelta(days=i % 30)
        
        sample_data.append({
            'id': f"ALL-{datetime.now().strftime('%Y%m')}-{str(i).zfill(4)}",
            'material_code': material['code'],
            'material_name': material['name'],
            'material_group': material['group'],
            'division': division,
            'requested_qty': requested_qty,
            'approved_qty': approved_qty,
            'unit': 'Nos',
            'request_date': date.isoformat(),
            'status': status,
            'requested_by': 'mofiz',
            'requested_by_name': 'Mofiz',
            'remarks': f'Sample request {i} - {status}',
            'priority': 'Normal'
        })
    
    return sample_data

# ================================================================
# EXISTING FILTER ENDPOINTS (CACHED)
# ================================================================

@api_bp.route('/api/filter-options')
@login_required
def get_filter_options():
    """Get filter options - CACHED for 1 hour"""
    try:
        cache_key = 'filter_options'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response({"zones": [{"id": "zone_siliguri", "name": "Siliguri Zone"}], "regions": [], "divisions": []})
        
        centers = list(db.centers.find({}))
        regions = sorted({c.get('region') for c in centers if c.get('region')})
        divisions = sorted({c.get('division') for c in centers if c.get('division')})
        
        response = {
            "zones": [{"id": "zone_siliguri", "name": "Siliguri Zone"}],
            "regions": [{"id": r, "name": r} for r in regions],
            "divisions": [{"id": d, "name": d} for d in divisions]
        }
        
        _cache.set(cache_key, response, ttl=3600)
        return safe_json_response(response)
    except Exception as e:
        return safe_json_response({"error": str(e)})

@api_bp.route('/api/zones')
@login_required
def get_zones():
    """Get zones - CACHED for 1 hour"""
    try:
        cache_key = 'zones_data'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([{"_id": "zone_siliguri", "name": "Siliguri Zone", "incharge": "Chief Engineer", "total_consumers": 0, "total_staff": 0, "total_dtr": 0, "center_count": 0}])
        
        centers = list(db.centers.find({}))
        if centers:
            response = [{
                "_id": "zone_siliguri",
                "name": "Siliguri Zone",
                "incharge": "Chief Engineer",
                "total_consumers": sum(c.get('total_consumers', 0) for c in centers),
                "total_staff": sum(c.get('total_staff', 0) for c in centers),
                "total_dtr": sum(c.get('total_dtr', 0) for c in centers),
                "center_count": len(centers)
            }]
            _cache.set(cache_key, response, ttl=3600)
            return safe_json_response(response)
        
        return safe_json_response([{"_id": "zone_siliguri", "name": "Siliguri Zone", "incharge": "Chief Engineer", "total_consumers": 0, "total_staff": 0, "total_dtr": 0, "center_count": 0}])
    except Exception as e:
        return safe_json_response({"error": str(e)})

# ================================================================
# INVENTORY ENDPOINTS (CACHED)
# ================================================================

@api_bp.route('/api/inventory/current-stock')
@login_required
def get_current_stock():
    """Get current stock - CACHED for 5 minutes"""
    try:
        cache_key = 'current_stock_data'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        print("📊 Fetching current stock from database...")
        stock = list(db.current_stock.find({}, {'_id': 0}))
        
        for item in stock:
            if 'plant' in item:
                item['plant'] = str(item['plant']).replace('.0', '')
        
        _cache.set(cache_key, stock, ttl=300)
        print(f"✅ Cached {len(stock)} stock records")
        
        return safe_json_response(stock)
    except Exception as e:
        print(f"Error in get_current_stock: {e}")
        return safe_json_response([])

@api_bp.route('/api/inventory/material-in-transit')
@login_required
def get_material_in_transit():
    """Get material in transit - CACHED for 2 minutes"""
    try:
        doc_type = request.args.get('doc_type', 'all')
        from_plant = request.args.get('from_plant', 'all')
        to_plant = request.args.get('to_plant', 'all')
        material_group = request.args.get('material_group', 'all')
        material_code = request.args.get('material_code', 'all')
        
        cache_key = f'transit_{doc_type}_{from_plant}_{to_plant}_{material_group}_{material_code}'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        query = {}
        if doc_type and doc_type != 'all':
            query['document_type'] = doc_type
        if from_plant and from_plant != 'all' and from_plant != 'Vendor':
            query['from_plant'] = {'$regex': from_plant, '$options': 'i'}
        if to_plant and to_plant != 'all':
            query['to_plant'] = {'$regex': to_plant, '$options': 'i'}
        if material_group and material_group != 'all':
            query['material_group'] = material_group
        if material_code and material_code != 'all':
            query['material_code'] = material_code
        
        print(f"🚚 Fetching transit data from database...")
        transit_items = list(db.material_in_transit.find(query, {'_id': 0}))
        
        material_group_map = {}
        all_material_codes = list(set([item.get('material_code') for item in transit_items if item.get('material_code')]))
        if all_material_codes:
            material_master_records = db.material_master.find(
                {'Material_Code': {'$in': all_material_codes}},
                {'Material_Code': 1, 'Material Group': 1}
            )
            for record in material_master_records:
                material_code_key = record.get('Material_Code')
                if material_code_key:
                    material_group_map[material_code_key] = record.get('Material Group', 'Uncategorized')
        
        for item in transit_items:
            material_code = item.get('material_code')
            if material_code and material_code in material_group_map:
                item['material_group'] = material_group_map[material_code]
            elif not item.get('material_group') or item.get('material_group') == 'Uncategorized':
                item['material_group'] = 'Uncategorized'
            
            if item.get('from_plant'):
                from_division = db.storage_locations.find_one({'plant': item.get('from_plant')}, {'division': 1})
                item['from_division'] = from_division.get('division', item.get('from_plant')) if from_division else item.get('from_plant')
            if item.get('to_plant'):
                to_division = db.storage_locations.find_one({'plant': item.get('to_plant')}, {'division': 1})
                item['to_division'] = to_division.get('division', item.get('to_plant')) if to_division else item.get('to_plant')
        
        _cache.set(cache_key, transit_items, ttl=120)
        print(f"✅ Cached {len(transit_items)} transit records")
        
        return safe_json_response(transit_items)
    except Exception as e:
        print(f"Error in get_material_in_transit: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response([])

@api_bp.route('/api/inventory/transit-summary')
@login_required
def get_transit_summary():
    """Get transit summary - CACHED for 2 minutes"""
    try:
        cache_key = 'transit_summary'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response({'total_items': 0, 'total_quantity': 0, 'by_doc_type': {}, 'delayed_count': 0})
        
        transit_items = list(db.material_in_transit.find({}))
        
        total_items = len(transit_items)
        total_quantity = sum(item.get('quantity', 0) for item in transit_items)
        
        by_doc_type = {}
        for item in transit_items:
            doc_type = item.get('document_type', 'Unknown')
            by_doc_type[doc_type] = by_doc_type.get(doc_type, 0) + 1
        
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
        
        response = {
            'total_items': total_items,
            'total_quantity': round(total_quantity, 2),
            'by_doc_type': by_doc_type,
            'delayed_count': delayed_count
        }
        
        _cache.set(cache_key, response, ttl=120)
        return safe_json_response(response)
    except Exception as e:
        print(f"Error in get_transit_summary: {e}")
        return safe_json_response({'total_items': 0, 'total_quantity': 0, 'by_doc_type': {}, 'delayed_count': 0})

# ================================================================
# CONSUMPTION ENDPOINTS (CACHED)
# ================================================================

@api_bp.route('/api/consumption/overview')
@login_required
def get_consumption_overview():
    """Get consumption overview - CACHED for 10 minutes"""
    try:
        cache_key = 'consumption_overview'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
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
            
            response = {
                'total_consumption': round(total, 2),
                'avg_monthly': round(total / 12, 2),
                'top_material': top_material,
                'active_plants': 12,
                'warnings': {'stock_only': 0, 'no_stock': 0}
            }
            _cache.set(cache_key, response, ttl=600)
            return safe_json_response(response)
        
        response = {
            'total_consumption': 0,
            'avg_monthly': 0,
            'top_material': 'No data',
            'active_plants': 0,
            'warnings': {'stock_only': 0, 'no_stock': 0}
        }
        _cache.set(cache_key, response, ttl=600)
        return safe_json_response(response)
        
    except Exception as e:
        print(f"Error in get_consumption_overview: {e}")
        return safe_json_response({
            'total_consumption': 0,
            'avg_monthly': 0,
            'top_material': 'Error',
            'active_plants': 0
        })

@api_bp.route('/api/consumption/material-groups')
@login_required
def get_consumption_material_groups():
    """Get material groups - CACHED for 1 hour"""
    try:
        cache_key = 'material_groups'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        groups = db.consumption_summary.distinct('material_group')
        excluded = ['monthly=', 'monthly=27', 'quarterly=3', 'qterly=3', 'yearly']
        groups = [g for g in groups if g and not any(ex in g.lower() for ex in excluded)]
        
        _cache.set(cache_key, sorted(groups), ttl=3600)
        return safe_json_response(sorted(groups))
    except Exception as e:
        print(f"Error in get_consumption_material_groups: {e}")
        return safe_json_response([])

# ★ FIXED: CONSUMPTION MATERIALS ENDPOINT WITH PROPER NaN HANDLING
@api_bp.route('/api/consumption/materials')
@login_required
def get_consumption_materials():
    """
    ★ FIXED: Get materials with names and weights from material_master
    Returns: material_code, material_name, unit, material_group, weight_per_unit_mt
    Properly handles NaN values by converting to None
    """
    try:
        group = request.args.get('group', None)
        cache_key = f'materials_{group if group else "all"}'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        materials = []
        
        # Build query
        query = {}
        if group and group != 'all' and group != 'undefined':
            query['Material Group'] = group
        
        # Get all records from material_master
        master_records = db.material_master.find(
            query,
            {
                'Material_Code': 1, 
                'Material Description': 1, 
                'Unit of Entry': 1, 
                'Material Group': 1, 
                'Unit Weight (MT)': 1
            }
        )
        
        for record in master_records:
            code = record.get('Material_Code')
            if not code:
                continue
            
            # Get material name
            material_name = record.get('Material Description', '')
            if not material_name or str(material_name).strip() == '':
                material_name = str(code)
            
            # ★ FIXED: Get weight and handle NaN properly
            weight_mt = record.get('Unit Weight (MT)', None)
            if weight_mt is not None and weight_mt != '':
                try:
                    weight_mt = float(weight_mt)
                    # Check for NaN
                    if math.isnan(weight_mt):
                        weight_mt = None
                except (ValueError, TypeError):
                    weight_mt = None
            else:
                weight_mt = None
            
            materials.append({
                'material_code': str(code),
                'material_name': str(material_name),
                'unit': str(record.get('Unit of Entry', 'NOS')),
                'material_group': str(record.get('Material Group', 'Uncategorized')),
                'weight_per_unit_mt': weight_mt
            })
        
        # If no materials from material_master, try consumption_summary as fallback
        if not materials:
            print("⚠️ No materials in material_master, trying consumption_summary...")
            try:
                pipeline = [
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
                
                if group and group != 'all' and group != 'undefined':
                    pipeline[0]['$match']['material_group'] = group
                
                materials_raw = list(db.consumption_summary.aggregate(pipeline))
                
                for m in materials_raw:
                    if m['_id']['code']:
                        materials.append({
                            'material_code': str(m['_id']['code']),
                            'material_name': str(m['_id']['name'] or m['_id']['code']),
                            'material_group': str(m['_id']['group'] or 'Uncategorized'),
                            'unit': str(m['_id']['unit'] or 'Units'),
                            'weight_per_unit_mt': None
                        })
            except Exception as e:
                print(f"Error fetching from consumption_summary: {e}")
        
        # Remove duplicates by material_code
        seen = set()
        unique_materials = []
        for m in materials:
            code = m['material_code']
            if code not in seen:
                seen.add(code)
                unique_materials.append(m)
        
        # Sort by material_name
        unique_materials.sort(key=lambda x: x.get('material_name', ''))
        
        # Log stats
        weight_count = len([m for m in unique_materials if m.get('weight_per_unit_mt') is not None])
        print(f"✅ Materials: {len(unique_materials)} total, {weight_count} with weight")
        
        # Cache for 30 minutes
        _cache.set(cache_key, unique_materials, ttl=1800)
        return safe_json_response(unique_materials)
        
    except Exception as e:
        print(f"Error in get_consumption_materials: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response([])

# ================================================================
# REST OF EXISTING ENDPOINTS (UNCHANGED)
# ================================================================

@api_bp.route('/api/consumption/plants')
@login_required
def get_consumption_plants():
    """Get plants - CACHED for 1 hour"""
    try:
        cache_key = 'consumption_plants'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        db = get_db()
        if db is None:
            return safe_json_response([])
        
        plants = db.consumption_summary.distinct('plant')
        plants = [p for p in plants if p and p != 'all']
        plant_list = [{'code': p, 'name': p} for p in sorted(plants)]
        
        _cache.set(cache_key, plant_list, ttl=3600)
        return safe_json_response(plant_list)
    except Exception as e:
        print(f"Error in get_consumption_plants: {e}")
        return safe_json_response([])

# ================================================================
# MONTHLY AVERAGE ENDPOINTS (CACHED)
# ================================================================

@api_bp.route('/api/consumption/monthly-average')
@login_required
def get_monthly_average_consumption():
    """Get monthly average - CACHED for 10 minutes"""
    try:
        plant = request.args.get('plant', 'all')
        material_group = request.args.get('material_group', 'all')
        material_code = request.args.get('material_code', 'all')
        
        cache_key = f'monthly_avg_{plant}_{material_group}_{material_code}'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
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
        
        _cache.set(cache_key, avg_map, ttl=600)
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
    """Get monthly average by plant - CACHED for 10 minutes"""
    try:
        plants_param = request.args.get('plants', '')
        material_group = request.args.get('material_group', 'all')
        material_code = request.args.get('material_code', 'all')
        
        cache_key = f'monthly_avg_plant_{plants_param}_{material_group}_{material_code}'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
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
        
        _cache.set(cache_key, avg_map, ttl=600)
        print(f"Returning averages for {len(avg_map)} materials")
        return safe_json_response(avg_map)
        
    except Exception as e:
        print(f"Error in get_monthly_average_by_plant: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response({})

# ================================================================
# CONSUMPTION DATA ENDPOINT (POST - Not Cached)
# ================================================================

@api_bp.route('/api/consumption/data', methods=['POST'])
@login_required
def get_consumption_data():
    """Get detailed consumption data with period-based aggregation"""
    try:
        data = request.json or {}
        period_type = data.get('period', 'monthly')
        plant = data.get('plant', 'all')
        material_group = data.get('material_group', 'all')
        material_code = data.get('material_code', 'all')
        
        db = get_db()
        if db is None:
            return safe_json_response({'consumption_data': [], 'summary': {'total_consumption': 0}})
        
        query = {}
        if plant and plant != 'all':
            query['plant'] = plant
        if material_group and material_group != 'all':
            query['material_group'] = material_group
        if material_code and material_code != 'all':
            query['material_code'] = material_code
        
        print(f"Consumption Data Query: {query}")
        
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

# ================================================================
# ADMIN DASHBOARD ENDPOINTS
# ================================================================

@api_bp.route('/api/admin/data', methods=['POST'])
@login_required
def get_admin_data():
    """Get admin data - CACHED for 5 minutes"""
    try:
        filters = request.json or {}
        cache_key = f'admin_data_{filters.get("region", "all")}_{filters.get("division", "all")}'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
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
        
        response = {
            'kpi': {'regions': unique_regions, 'divisions': unique_divisions, 'substations': total_dtr, 'staff': total_staff, 'consumers': total_consumers},
            'regions': regions_data,
            'divisions': divisions_data
        }
        
        _cache.set(cache_key, response, ttl=300)
        return safe_json_response(response)
    except Exception as e:
        return safe_json_response({"error": str(e)})

# ================================================================
# LEGACY ENDPOINTS
# ================================================================

@api_bp.route('/api/dashboard/administrative', methods=['POST'])
@login_required
def get_dashboard_administrative():
    return get_admin_data()

@api_bp.route('/api/priority-works/overview')
@login_required
def get_priority_works_overview():
    """Priority works overview - CACHED for 1 hour"""
    try:
        cache_key = 'priority_works'
        cached_data = _cache.get(cache_key)
        if cached_data is not None:
            return safe_json_response(cached_data)
        
        response = {
            'hvds': {'total': 8, 'completed': 3, 'in_progress': 5, 'percentage': '65%', 'budget': '₹8.2Cr'},
            'newSubstation': {'total': 3, 'capacity': '120 MVA', 'progress': '45%', 'budget': '₹6.5Cr', 'target': 'Dec 2026'},
            'ptr': {'total': 12, 'completed': 5, 'capacity': '85 MVA', 'progress': '42%', 'budget': '₹4.8Cr'},
            'new33kv': {'count': 2, 'length': '28 km', 'towers': 84, 'budget': '₹3.2Cr', 'start': 'Apr 2026'},
            'cond33kv': {'count': 8, 'length': '42 km', 'completed': 3, 'progress': '38%', 'budget': '₹2.8Cr'},
            'new11kv': {'count': 5, 'length': '15 km', 'poles': 225, 'budget': '₹1.8Cr', 'start': 'May 2026'},
            'cond11kv': {'count': 12, 'length': '38 km', 'completed': 12, 'progress': '100%', 'budget': '₹1.2Cr'}
        }
        _cache.set(cache_key, response, ttl=3600)
        return safe_json_response(response)
    except Exception as e:
        return safe_json_response({'error': str(e)})

# ================================================================
# ★ NEW: LAST UPDATED TIMESTAMP ENDPOINT
# ================================================================

@api_bp.route('/api/last-updated')
@login_required
def get_last_updated():
    """Get the last data sync timestamp from MongoDB"""
    try:
        db = get_db()
        if db is None:
            return safe_json_response({'last_updated': None})
        
        # Get the latest record with last_updated field
        # Check multiple collections for the most recent timestamp
        collections_to_check = ['current_stock', 'consumption_summary', 'material_in_transit', 'material_master']
        latest_date = None
        latest_collection = None
        
        for coll_name in collections_to_check:
            try:
                collections = db.list_collection_names()
                if coll_name in collections:
                    # Find the most recent record in this collection
                    latest = db[coll_name].find_one(
                        {}, 
                        {'last_updated': 1, '_id': 0},
                        sort=[('last_updated', -1)]
                    )
                    if latest and latest.get('last_updated'):
                        if not latest_date or latest['last_updated'] > latest_date:
                            latest_date = latest['last_updated']
                            latest_collection = coll_name
            except Exception as e:
                print(f"Error checking {coll_name}: {e}")
                continue
        
        if latest_date:
            # Format the timestamp
            if isinstance(latest_date, datetime):
                formatted = latest_date.strftime('%B %d, %Y at %I:%M:%S %p')
            elif isinstance(latest_date, str):
                # Try to parse it
                try:
                    from dateutil import parser
                    dt = parser.parse(latest_date)
                    formatted = dt.strftime('%B %d, %Y at %I:%M:%S %p')
                except:
                    formatted = latest_date
            else:
                formatted = str(latest_date)
            
            print(f"✅ Last updated from {latest_collection}: {formatted}")
            return safe_json_response({
                'last_updated': formatted, 
                'raw': latest_date.isoformat() if isinstance(latest_date, datetime) else latest_date,
                'collection': latest_collection
            })
        
        print("⚠️ No last_updated timestamp found in any collection")
        return safe_json_response({'last_updated': None})
        
    except Exception as e:
        print(f"Error in get_last_updated: {e}")
        import traceback
        traceback.print_exc()
        return safe_json_response({'last_updated': None})