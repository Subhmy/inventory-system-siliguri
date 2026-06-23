"""
Decorators for route protection
"""

from functools import wraps
from flask import session, flash, redirect, url_for

def login_required(f):
    """Decorator to require user to be logged in"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash('Please log in first', 'warning')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    """
    Decorator to require specific user role(s)
    
    Usage:
        @role_required('admin')           # Admin only
        @role_required('admin', 'manager') # Admin or Manager
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                flash('Please log in first', 'warning')
                return redirect(url_for('auth.login'))
            
            user = session.get('user', {})
            user_role = user.get('role', '').lower()
            
            # DEBUG: Print to console
            print(f"🔐 ROLE CHECK: user='{user.get('username')}', role='{user_role}', allowed={roles}")
            
            if user_role not in roles:
                flash(f'Access denied. Required: {roles}, Your role: {user_role}', 'danger')
                return redirect(url_for('main.general_overview'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator