from app import create_app, db
from app.models import User, Role

app = create_app()

@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'User': User, 'Role': Role}

@app.cli.command("setup-roles")
def setup_roles():
    """Initializes roles if the roles table is empty."""
    if Role.query.count() == 0:
        print("No roles found. Initializing roles...")
        roles = ['Viewer', 'Admin', 'Owner']
        for r in roles:
            role = Role(name=r)
            db.session.add(role)
        db.session.commit()
        print("Roles successfully initialized.")
    else:
        print("Roles already exist.")

@app.cli.command("create-owner")
def create_owner():
    """Create an owner user."""
    if Role.query.count() == 0:
        print("Roles not initialized. Please run 'flask setup-roles' first.")
        return

    owner_role = Role.query.filter_by(name='Owner').first()
    if not owner_role:
        print("Owner role not found. Please run 'flask setup-roles' first.")
        return

    # Check if owner user already exists
    existing_owner = User.query.filter_by(username='admin_owner').first()
    if existing_owner:
        print("Owner user 'admin_owner' already exists.")
        return

    owner_user = User(
        username='admin_owner',
        email='admin.owner@financecorp.com',
        role=owner_role
    )
    owner_user.set_password('AdminPass456!')

    db.session.add(owner_user)
    db.session.commit()

    print("Owner user 'admin_owner' created successfully.")

@app.cli.command("create-demo-user")
def create_demo_user():
    """Create a demo user."""
    if Role.query.count() == 0:
        print("Roles not initialized. Please run 'flask setup-roles' first.")
        return

    # Check if demo user exists
    test_user = User.query.filter_by(username='demo').first()
    if test_user:
        print("Demo user 'demo' already exists.")
        return

    # Create demo user
    viewer_role = Role.query.filter_by(name='Viewer').first()
    if not viewer_role:
        print("Error: Viewer role not found. Please run 'flask setup-roles' first.")
        return

    demo_user = User(
        username='demo',
        email='demo@test.com',
        role=viewer_role
    )
    demo_user.set_password('Demo@123!')
    db.session.add(demo_user)
    db.session.commit()
    print("Demo user 'demo' created successfully!")
    print("Username: demo")
    print("Password: Demo@123!")

@app.cli.command("reset-demo-user")
def reset_demo_user():
    """Resets the demo user."""
    if Role.query.count() == 0:
        print("Roles not initialized. Please run 'flask setup-roles' first.")
        return

    # Delete existing demo user
    demo_user = User.query.filter_by(username='demo').first()
    if demo_user:
        db.session.delete(demo_user)
        db.session.commit()
        print("Existing demo user deleted.")

    # Create new demo user
    viewer_role = Role.query.filter_by(name='Viewer').first()
    if not viewer_role:
        print("Error: Viewer role not found. Please run 'flask setup-roles' first.")
        return

    new_demo_user = User(
        username='demo',
        email='demo@test.com',
        role=viewer_role
    )
    new_demo_user.set_password('Demo@123!')
    db.session.add(new_demo_user)
    db.session.commit()
    print("Demo user has been reset.")
    print("Username: demo")
    print("Password: Demo@123!")

@app.cli.command("setup-demo-data")
def setup_demo_data():
    """Sets up test data for the demo user."""
    from datetime import datetime, timedelta

    # Get demo user
    demo_user = User.query.filter_by(username='demo').first()
    if not demo_user:
        print("Demo user not found. Please run 'flask create-demo-user' first.")
        return

    print(f"Found demo user: {demo_user.username}")

    # Clear existing data
    PaymentData.query.filter_by(user_id=demo_user.id).delete()
    IBRebate.query.filter_by(user_id=demo_user.id).delete()
    CRMWithdrawals.query.filter_by(user_id=demo_user.id).delete()
    CRMDeposit.query.filter_by(user_id=demo_user.id).delete()
    AccountList.query.filter_by(user_id=demo_user.id).delete()

    now = datetime.now()

    payment_data = [
        ('M2P_DEP_001', 'M2p Deposit', 'DEPOSIT', 500.25, 25.50), ('M2P_DEP_002', 'M2p Deposit', 'DEPOSIT', 750.00, 35.75),
        ('SET_DEP_001', 'Settlement Deposit', 'DEPOSIT', 1500.00, 75.00), ('M2P_WITH_001', 'M2p Withdraw', 'WITHDRAW', 300.00, 15.00)
    ]
    for i, (tx_id, category, tx_type, amount, fee) in enumerate(payment_data):
        payment = PaymentData(user_id=demo_user.id, tx_id=tx_id, status='DONE', type=tx_type, sheet_category=category, final_amount=amount, tier_fee=fee, created=now - timedelta(days=i))
        db.session.add(payment)

    rebate_data = [125.50, 89.75]
    for i, rebate_amount in enumerate(rebate_data):
        rebate = IBRebate(user_id=demo_user.id, transaction_id=f'REBATE_{i+1:03d}', rebate=rebate_amount, rebate_time=now - timedelta(days=i))
        db.session.add(rebate)

    crm_deposit_data = [(850.00, 'CARD'), (1250.75, 'TOPCHANGE')]
    for i, (amount, method) in enumerate(crm_deposit_data):
        crm_deposit = CRMDeposit(user_id=demo_user.id, request_id=f'CRM_DEP_{i+1:03d}', trading_amount=amount, payment_method=method, client_id=f'CLIENT_{i+1000}', name=f'Client Name {i+1}', request_time=now - timedelta(days=i))
        db.session.add(crm_deposit)

    db.session.commit()
    print("Demo data created successfully.")

if __name__ == '__main__':
    # When running with a production WSGI server like Gunicorn,
    # this block is not executed. The port and host are configured
    # in the WSGI server's command.
    # For `flask run`, the host and port are specified in the Dockerfile's CMD.
    app.run(host='0.0.0.0', port=5001, debug=False)
