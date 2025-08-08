#!/usr/bin/env python
from app import create_app, db
from app.models import User, Role

app = create_app()

with app.app_context():
    # Create all database tables
    db.create_all()
    
    # Initialize roles if they don't exist
    if Role.query.count() == 0:
        print("Creating roles...")
        roles = ['Viewer', 'Admin', 'Owner']
        for role_name in roles:
            role = Role(name=role_name)
            db.session.add(role)
        db.session.commit()
        print("Roles created successfully.")
    else:
        print("Roles already exist.")
    
    print("Database initialization complete.")