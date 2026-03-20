from flask import Blueprint, render_template, request, session, flash, redirect, url_for

auth_bp = Blueprint('auth', __name__)

USERS = {
    'admin@siliguri.com': {'password': 'admin123', 'role': 'admin'},
    'manager@siliguri.com': {'password': 'manager123', 'role': 'manager'},
    'user@siliguri.com': {'password': 'user123', 'role': 'user'}
}

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        if email in USERS and USERS[email]['password'] == password:
            session['user'] = {'email': email, 'role': USERS[email]['role']}
            session.permanent = True
            flash(f'Welcome back, {email}!', 'success')
            return redirect(url_for('main.general_overview'))
        else:
            flash('Invalid email or password', 'danger')
    
    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out', 'info')
    return redirect(url_for('auth.login'))