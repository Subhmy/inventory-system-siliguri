from app.models.mongo_utils import get_db

db = get_db()
if db is not None:
    print("=== CHECKING CONDUCTOR DATA STRUCTURE ===")
    
    # Get one CONDUCTOR record
    conductor_sample = db.consumption_summary.find_one({'material_group': 'CONDUCTOR'})
    if conductor_sample:
        print("\nSample CONDUCTOR record fields:")
        for key, value in conductor_sample.items():
            print(f"  {key}: {value}")
    
    # Check if plant field exists in CONDUCTOR records
    conductor_with_plant = db.consumption_summary.count_documents({'material_group': 'CONDUCTOR', 'plant': {'$exists': True, '$ne': None}})
    conductor_without_plant = db.consumption_summary.count_documents({'material_group': 'CONDUCTOR', 'plant': {'$exists': False}})
    
    print(f"\nCONDUCTOR records with plant field: {conductor_with_plant}")
    print(f"CONDUCTOR records without plant field: {conductor_without_plant}")
    
    # Check what plant values exist for CONDUCTOR
    conductor_plants = db.consumption_summary.distinct('plant', {'material_group': 'CONDUCTOR'})
    print(f"\nPlants in CONDUCTOR records: {conductor_plants}")
    
    # Check for material 502010921 in any collection
    print(f"\nMaterial 502010921 in consumption_summary: {db.consumption_summary.count_documents({'material_code': '502010921'})}")
    
    # Check inventory_transactions for this material
    print(f"Material 502010921 in inventory_transactions: {db.inventory_transactions.count_documents({'material_code': '502010921'})}")
    
    # Show sample from inventory_transactions for CONDUCTOR
    trans_sample = db.inventory_transactions.find_one({'material_group': 'CONDUCTOR'})
    if trans_sample:
        print("\nSample from inventory_transactions:")
        for key, value in trans_sample.items():
            print(f"  {key}: {value}")