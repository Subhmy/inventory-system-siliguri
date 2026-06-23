"""
User Management from Google Sheets
Fetches and caches user credentials from Google Sheet
"""

import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

class UserManager:
    def __init__(self, sheet_id, sheet_gid=129186434):
        self.sheet_id = sheet_id
        self.sheet_gid = sheet_gid
        self.users = {}
        self.last_fetch = None
        self.cache_duration = timedelta(minutes=5)  # Refresh every 5 minutes
        
    def fetch_users(self, force_refresh=False):
        """Fetch users from Google Sheet"""
        
        # Check cache
        if not force_refresh and self.last_fetch:
            if datetime.now() - self.last_fetch < self.cache_duration:
                print(f"📦 Using cached users ({len(self.users)} users)")
                return self.users
        
        try:
            # Google Sheet CSV export URL
            url = f"https://docs.google.com/spreadsheets/d/{self.sheet_id}/export?format=csv&gid={self.sheet_gid}"
            
            print(f"📥 Fetching users from Google Sheet...")
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                print(f"❌ Failed to fetch users: {response.status_code}")
                return self.users
            
            # Parse CSV
            df = pd.read_csv(StringIO(response.text))
            print(f"   📋 Columns found: {list(df.columns)}")
            
            # Convert to dictionary using Username
            self.users = {}
            for _, row in df.iterrows():
                # Look for Username column
                username = None
                for col in ['Username', 'username', 'User', 'user']:
                    if col in df.columns:
                        username = str(row.get(col, '')).strip().lower()
                        break
                
                if not username:
                    continue
                
                # Get Role
                role = 'user'
                for col in ['Role', 'role', 'ROLE']:
                    if col in df.columns:
                        role = str(row.get(col, 'user')).strip().lower()
                        break
                
                # Get Password
                password = ''
                for col in ['Password', 'password', 'PASSWORD']:
                    if col in df.columns:
                        password = str(row.get(col, '')).strip()
                        break
                
                # Get Name
                name = username
                for col in ['Name', 'name', 'NAME']:
                    if col in df.columns:
                        name = str(row.get(col, username)).strip()
                        break
                
                # Get Active status
                active = True
                for col in ['Active', 'active', 'ACTIVE']:
                    if col in df.columns:
                        active_val = str(row.get(col, 'TRUE')).strip().upper()
                        active = active_val == 'TRUE'
                        break
                
                if username and password and active:
                    self.users[username] = {
                        'password': password,
                        'role': role,
                        'name': name,
                        'active': active
                    }
            
            self.last_fetch = datetime.now()
            print(f"✅ Loaded {len(self.users)} users from Google Sheet")
            
            # Print users for debugging
            for username, data in self.users.items():
                print(f"   👤 {username} → {data['role']} ({data['name']})")
            
            return self.users
            
        except Exception as e:
            print(f"❌ Error fetching users: {e}")
            import traceback
            traceback.print_exc()
            return self.users
    
    def get_user(self, username):
        """Get user by username"""
        username = username.strip().lower()
        if not self.users:
            self.fetch_users()
        return self.users.get(username)
    
    def validate_user(self, username, password):
        """Validate user credentials"""
        username = username.strip().lower()
        user = self.get_user(username)
        
        if not user:
            return None
        
        if user.get('password') == password and user.get('active', True):
            return {
                'username': username,
                'role': user.get('role', 'user'),
                'name': user.get('name', username)
            }
        
        return None
    
    def refresh(self):
        """Force refresh users from Google Sheet"""
        return self.fetch_users(force_refresh=True)
    
    def get_all_users(self):
        """Get all active users"""
        if not self.users:
            self.fetch_users()
        return {k: v for k, v in self.users.items() if v.get('active', True)}


# Singleton instance
_user_manager = None

def get_user_manager():
    """Get singleton UserManager instance"""
    global _user_manager
    if _user_manager is None:
        sheet_id = os.getenv('USER_SHEET_ID')
        sheet_gid = int(os.getenv('USER_SHEET_GID', 129186434))
        
        if not sheet_id:
            print("⚠️ USER_SHEET_ID not found in .env, using default users")
            return None
        
        _user_manager = UserManager(sheet_id, sheet_gid)
    return _user_manager