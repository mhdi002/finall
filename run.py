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
    """Sets up comprehensive test data for the demo user."""
    from datetime import datetime, timedelta
    from app.models import PaymentData, IBRebate, CRMWithdrawals, CRMDeposit, AccountList

    demo_user = User.query.filter_by(username='demo').first()
    if not demo_user:
        print("Demo user not found. Please run 'flask create-demo-user' first.")
        return

    print(f"Found demo user: {demo_user.username}. Clearing old data...")

    # Clear existing data for the demo user
    PaymentData.query.filter_by(user_id=demo_user.id).delete()
    IBRebate.query.filter_by(user_id=demo_user.id).delete()
    CRMWithdrawals.query.filter_by(user_id=demo_user.id).delete()
    CRMDeposit.query.filter_by(user_id=demo_user.id).delete()
    AccountList.query.filter_by(user_id=demo_user.id).delete()
    db.session.commit()

    now = datetime.utcnow()
    print("Generating new demo data...")

    # 1. Account List
    accounts = [
        {'login': '1001', 'name': 'John Doe', 'group': 'REAL\\Standard', 'is_welcome_bonus': False},
        {'login': '1002', 'name': 'Jane Smith', 'group': 'WELCOME\\Welcome BBOOK', 'is_welcome_bonus': True},
        {'login': '1003', 'name': 'Peter Jones', 'group': 'REAL\\ECN', 'is_welcome_bonus': False}
    ]
    for acc_data in accounts:
        account = AccountList(user_id=demo_user.id, **acc_data)
        db.session.add(account)
    print(f"- Created {len(accounts)} accounts")

    # 2. CRM Deposits (to test discrepancies)
    crm_deposits = [
        # This one will match a payment data record
        {'request_id': 'CRM_DEP_001', 'trading_amount': 500.00, 'payment_method': 'CARD', 'client_id': 'C1001', 'name': 'John Doe', 'request_time': now - timedelta(hours=1), 'trading_account': 'TA-1001'},
        # This one will NOT match (TopChange is ignored in discrepancy report)
        {'request_id': 'CRM_DEP_002', 'trading_amount': 1250.75, 'payment_method': 'TOPCHANGE', 'client_id': 'C1003', 'name': 'Peter Jones', 'request_time': now - timedelta(days=1), 'trading_account': 'TA-1003'},
        # This one will be a discrepancy
        {'request_id': 'CRM_DEP_003', 'trading_amount': 300.00, 'payment_method': 'WIRE', 'client_id': 'C1004', 'name': 'Missing Payment', 'request_time': now - timedelta(days=2), 'trading_account': 'TA-1004'}
    ]
    for dep_data in crm_deposits:
        deposit = CRMDeposit(user_id=demo_user.id, **dep_data)
        db.session.add(deposit)
    print(f"- Created {len(crm_deposits)} CRM deposits")

    # 3. Payment Data (to test discrepancies)
    payment_entries = [
        # This one will match CRM_DEP_001
        {'tx_id': 'M2P_DEP_001', 'sheet_category': 'M2p Deposit', 'type': 'DEPOSIT', 'final_amount': 500.25, 'tier_fee': 25.50, 'created': now - timedelta(hours=1, minutes=5), 'trading_account': 'Client C1001 in account TA-1001'},
        # This one will be a discrepancy
        {'tx_id': 'M2P_DEP_002', 'sheet_category': 'M2p Deposit', 'type': 'DEPOSIT', 'final_amount': 750.00, 'tier_fee': 35.75, 'created': now - timedelta(days=3), 'trading_account': 'Client C1005 in account TA-1005'},
        # Other types of payments
        {'tx_id': 'SET_DEP_001', 'sheet_category': 'Settlement Deposit', 'type': 'DEPOSIT', 'final_amount': 1500.00, 'tier_fee': 75.00, 'created': now - timedelta(days=4)},
        {'tx_id': 'M2P_WITH_001', 'sheet_category': 'M2p Withdraw', 'type': 'WITHDRAW', 'final_amount': 300.00, 'tier_fee': 15.00, 'created': now - timedelta(days=5)}
    ]
    for p_data in payment_entries:
        payment = PaymentData(user_id=demo_user.id, status='DONE', **p_data)
        db.session.add(payment)
    print(f"- Created {len(payment_entries)} payment data records")

    # 4. CRM Withdrawals (including one from a Welcome Bonus account)
    crm_withdrawals = [
        # Regular withdrawal
        {'request_id': 'CRM_WITH_001', 'withdrawal_amount': 100.00, 'review_time': now - timedelta(days=6), 'trading_account': '1001'},
        # Welcome bonus withdrawal
        {'request_id': 'CRM_WITH_002', 'withdrawal_amount': 50.00, 'review_time': now - timedelta(days=7), 'trading_account': '1002'}
    ]
    for w_data in crm_withdrawals:
        withdrawal = CRMWithdrawals(user_id=demo_user.id, **w_data)
        db.session.add(withdrawal)
    print(f"- Created {len(crm_withdrawals)} CRM withdrawals")

    # 5. IB Rebates
    rebate_data = [125.50, 89.75, 210.20]
    for i, rebate_amount in enumerate(rebate_data):
        rebate = IBRebate(user_id=demo_user.id, transaction_id=f'REBATE_{i+1:03d}', rebate=rebate_amount, rebate_time=now - timedelta(days=i+8))
        db.session.add(rebate)
    print(f"- Created {len(rebate_data)} IB rebates")

    db.session.commit()
    print("\nComprehensive demo data created successfully!")

if __name__ == '__main__':
    # When running with a production WSGI server like Gunicorn,
    # this block is not executed. The port and host are configured
    # in the WSGI server's command.
    # For `flask run`, the host and port are specified in the Dockerfile's CMD.
    app.run(host='0.0.0.0', port=5001, debug=False)
