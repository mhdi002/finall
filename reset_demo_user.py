#!/usr/bin/env python
from app import create_app, db
from app.models import User, Role

app = create_app()

with app.app_context():
    # Delete existing demo user
    demo_user = User.query.filter_by(username='demo').first()
    if demo_user:
        db.session.delete(demo_user)
        db.session.commit()
        print("Existing demo user deleted")
    
    # Create new demo user with proper password
    viewer_role = Role.query.filter_by(name='Viewer').first()
    demo_user = User(
        username='demo',
        email='demo@test.com', 
        role=viewer_role
    )
    demo_user.set_password('Demo@123!')
    db.session.add(demo_user)
    db.session.commit()
    print("Demo user created with strong password")
    print("Username: demo")
    print("Password: Demo@123!")