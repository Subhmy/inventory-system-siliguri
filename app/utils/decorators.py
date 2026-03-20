from functools import wraps
from flask import session, flash, redirect, url_for

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash('Please log in first', 'warning')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                flash('Please log in first', 'warning')
                return redirect(url_for('auth.login'))
            
            user_role = session['user'].get('role')
            if user_role not in roles:
                flash('You do not have permission', 'danger')
                return redirect(url_for('main.general_overview'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator