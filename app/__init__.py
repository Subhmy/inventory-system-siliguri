from flask import Flask, session
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_app():
    """Create and configure Flask app"""
    # Get the absolute path to the templates folder
    template_dir = os.path.abspath('templates')
    
    # Create app with explicit template folder
    app = Flask(__name__, template_folder=template_dir)
    
    # Configuration
    app.secret_key = os.getenv('FLASK_SECRET_KEY', 'default-secret-key')
    app.config['PERMANENT_SESSION_LIFETIME'] = 86400  # 1 day
    
    # Register blueprints
    from app.routes.auth import auth_bp
    from app.routes.main import main_bp
    from app.routes.api import api_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp)
    
    # ================================================================
    # CONTEXT PROCESSOR - Makes user available to ALL templates
    # ================================================================
    @app.context_processor
    def inject_user():
        """Inject user into all templates"""
        return dict(user=session.get('user'))
    
    # ================================================================
    # DEBUG: Print template folder and registered routes
    # ================================================================
    print("\n" + "=" * 60)
    print("📁 Templates folder:", template_dir)
    print(f"📁 Login.html exists: {os.path.exists(os.path.join(template_dir, 'login.html'))}")
    
    print("\n📋 REGISTERED ROUTES:")
    print("=" * 60)
    for rule in app.url_map.iter_rules():
        print(f"   {rule.endpoint}: {rule.rule}")
    print("=" * 60 + "\n")
    
    return app