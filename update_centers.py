# update_from_google_sheets.py
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import re

# ===== CONFIGURATION - CHANGE THESE =====
MONGO_URI = "mongodb+srv://madhusudanback_db_user:Rahul1234G21mC2Roow36LPh@cluster0.5nt5nwr.mongodb.net/"
DB_NAME = "siliguri_electrical"  # Your database name
COLLECTION_NAME = "centers"       # Your collection name

# Your Google Sheet URL
SHEET_URL = "https://docs.google.com/spreadsheets/d/12ifSl_us1fzuLuQ74dUPnZbP8nlLt9ceMcloBMVUFeU/edit"

# ===== STEP 1: CONNECT TO MONGODB =====
print("🔌 Connecting to MongoDB...")
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("✅ MongoDB connected successfully!")
    
    # Test connection
    collection.count_documents({})
    print(f"📊 Found {collection.count_documents({})} existing centers")
except Exception as e:
    print(f"❌ MongoDB connection failed: {e}")
    exit()

# ===== STEP 2: READ GOOGLE SHEET =====
print("\n🔌 Connecting to Google Sheets...")
try:
    # Extract sheet ID from URL
    sheet_id = SHEET_URL.split('/')[5]
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    
    # Read the sheet
    df = pd.read_csv(csv_url)
    print(f"✅ Google Sheet read successfully!")
    print(f"📋 Found {len(df)} rows of data")
    print(f"📋 Columns detected: {list(df.columns)}")
except Exception as e:
    print(f"❌ Failed to read Google Sheet: {e}")
    exit()

# ===== STEP 3: DEFINE COLUMN MAPPING (YOUR EXACT COLUMNS) =====
column_mapping = {
    # MongoDB field    :   Your Google Sheet column
    'name': 'CCC Name',
    'zone': 'Zone',
    'region': 'Region',
    'division': 'Division',
    'ccc_type': 'CCC Type',
    'office_address': 'Office Address',
    'working_area': 'Working Area (sq km)',
    'total_consumers': 'Total Consumers',
    'consumers_domestic': 'Domestic',
    'consumers_commercial': 'Commercial',
    'consumers_industrial': 'Industrial',
    'consumers_agricultural': 'Agricultural',
    'consumers_temp_disconnected': 'Temp Disconnected',
    'consumers_perm_disconnected': 'Perm Disconnected',
    'total_dtr': 'Total DTR',
    'atc_loss_last_month': 'AT&C Loss % (Last March)',
    'atc_loss_running_month': 'AT&C Loss % (This Month)',
    'td_loss_last_month': 'T&D Loss % (Last March)',
    'td_loss_running_month': 'T&D Loss % (This Month)',
}

# ===== STEP 4: VERIFY ALL COLUMNS EXIST =====
print("\n🔍 Verifying column mapping...")
missing_columns = []
for mongo_field, sheet_col in column_mapping.items():
    if sheet_col not in df.columns:
        missing_columns.append(sheet_col)
        print(f"⚠️  Missing column in sheet: '{sheet_col}'")
    else:
        print(f"✅ Found: '{sheet_col}'")

if missing_columns:
    print(f"\n❌ Missing columns: {missing_columns}")
    print("Please check your Google Sheet has these columns exactly as named")
    exit()
else:
    print("✅ All columns found!")

# ===== STEP 5: PROCESS EACH ROW =====
print("\n🔄 Updating MongoDB...")
updated_count = 0
added_count = 0
error_count = 0
skipped_count = 0

for index, row in df.iterrows():
    try:
        # Get center name (required field)
        center_name = row['CCC Name']
        if pd.isna(center_name):
            print(f"⚠️  Row {index+2}: No center name, skipping")
            skipped_count += 1
            continue
        
        # Create unique ID from center name
        center_id = f"ccc_{str(center_name).lower().replace(' ', '_').replace('-', '_').replace('.', '')}"
        
        # Extract date if available
        data_month = '2026-03'  # Default
        if 'Date' in df.columns and not pd.isna(row['Date']):
            try:
                date_str = str(row['Date'])
                # Try to extract year-month
                if '-' in date_str:
                    parts = date_str.split('-')
                    if len(parts) >= 2:
                        data_month = f"{parts[2]}-{parts[1]}" if len(parts[2]) == 4 else f"2026-{parts[1]}"
            except:
                pass
        
        # Build the document
        document = {
            "_id": center_id,
            "name": center_name,
            "data_month": data_month,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        # Add all mapped fields
        for mongo_field, sheet_col in column_mapping.items():
            if sheet_col in df.columns and not pd.isna(row[sheet_col]):
                value = row[sheet_col]
                
                # Convert to appropriate type based on field name
                if 'loss' in mongo_field or 'percentage' in mongo_field:
                    try:
                        # Remove % sign if present and convert to float
                        if isinstance(value, str):
                            value = value.replace('%', '').strip()
                        document[mongo_field] = float(value)
                    except:
                        document[mongo_field] = 0.0
                        
                elif 'consumers' in mongo_field or 'dtr' in mongo_field or 'staff' in mongo_field:
                    try:
                        # Remove commas if present
                        if isinstance(value, str):
                            value = value.replace(',', '')
                        document[mongo_field] = int(float(value))
                    except:
                        document[mongo_field] = 0
                        
                else:
                    # String fields
                    document[mongo_field] = str(value)
        
        # Check if center exists
        existing = collection.find_one({"_id": center_id})
        
        if existing:
            # Update existing
            # Remove fields we don't want to overwrite
            update_data = {k: v for k, v in document.items() 
                          if k not in ['_id', 'created_at', 'name']}
            update_data['updated_at'] = datetime.now()
            
            result = collection.update_one(
                {"_id": center_id},
                {"$set": update_data}
            )
            if result.modified_count > 0:
                updated_count += 1
                print(f"🔄 Updated: {center_name}")
            else:
                print(f"⏸️  No changes: {center_name}")
        else:
            # Insert new
            result = collection.insert_one(document)
            added_count += 1
            print(f"➕ Added new: {center_name}")
            
    except Exception as e:
        error_count += 1
        print(f"❌ Error at row {index+2} ({center_name if 'center_name' in locals() else 'Unknown'}): {e}")

# ===== STEP 6: CREATE INDEXES =====
print("\n🔧 Creating indexes for faster queries...")
try:
    collection.create_index("zone")
    collection.create_index("region")
    collection.create_index("division")
    collection.create_index("name")
    collection.create_index("data_month")
    print("✅ Indexes created successfully")
except Exception as e:
    print(f"⚠️  Index creation warning: {e}")

# ===== STEP 7: SHOW SUMMARY =====
print("\n" + "="*60)
print("📊 UPDATE SUMMARY")
print("="*60)
print(f"✅ Added new centers: {added_count}")
print(f"🔄 Updated existing centers: {updated_count}")
print(f"⏸️  Skipped (no changes): {skipped_count}")
print(f"❌ Errors: {error_count}")
print(f"📊 Total in database now: {collection.count_documents({})}")
print("="*60)

# Show sample of updated data
print("\n📋 Sample of updated centers:")
sample = collection.find().limit(3)
for doc in sample:
    print(f"  • {doc.get('name')} - Consumers: {doc.get('total_consumers', 0)}")

client.close()
print("\n🔌 MongoDB connection closed")
print("✅ All done!")