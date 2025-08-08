#!/usr/bin/env python3
"""
Test admin functionality with Owner role
"""

import sys
sys.path.append('/app')

from app import create_app, db
from app.models import User, Role

def create_owner_user():
    """Create an Owner user for testing admin functionality"""
    app = create_app()
    with app.app_context():
        # Check if owner user already exists
        existing_owner = User.query.filter_by(username='admin_owner').first()
        if existing_owner:
            print("Owner user already exists")
            return True
            
        # Get Owner role
        owner_role = Role.query.filter_by(name='Owner').first()
        if not owner_role:
            print("Owner role not found in database")
            return False
            
        # Create owner user
        owner_user = User(
            username='admin_owner',
            email='admin.owner@financecorp.com',
            role=owner_role
        )
        owner_user.set_password('AdminPass456!')
        
        db.session.add(owner_user)
        db.session.commit()
        
        print("Owner user created successfully")
        return True

if __name__ == "__main__":
    create_owner_user()