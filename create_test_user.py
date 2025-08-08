#!/usr/bin/env python
"""
Create a test user for testing the web interface
"""
from app import create_app, db
from app.models import User, Role

app = create_app()

with app.app_context():
    # Check if test user exists
    test_user = User.query.filter_by(username='demo').first()
    if test_user:
        print("Demo user already exists")
    else:
        # Create demo user
        viewer_role = Role.query.filter_by(name='Viewer').first()
        if not viewer_role:
            print("Error: Viewer role not found")
            exit(1)
        
        demo_user = User(
            username='demo',
            email='demo@test.com',
            role=viewer_role
        )
        demo_user.set_password('Demo@123!')
        db.session.add(demo_user)
        db.session.commit()
        print("Demo user created successfully!")
        print("Username: demo")
        print("Password: Demo@123!")