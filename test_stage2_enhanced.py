#!/usr/bin/env python
"""
Test script for Stage 2 enhanced functionality
"""
from app import create_app, db
from app.models import User, Role, PaymentData, IBRebate, CRMWithdrawals, CRMDeposit, AccountList
from app.stage2_reports_enhanced import check_data_sufficiency_for_charts, generate_final_report
from datetime import datetime, timedelta
from flask_login import login_user
import pandas as pd

app = create_app()

def create_test_user():
    """Create a test user if not exists"""
    with app.app_context():
        test_user = User.query.filter_by(username='testuser').first()
        if not test_user:
            viewer_role = Role.query.filter_by(name='Viewer').first()
            test_user = User(username='testuser', email='test@test.com', role=viewer_role)
            test_user.set_password('testpass')
            db.session.add(test_user)
            db.session.commit()
            print("Test user created")
        else:
            print("Test user already exists")
        return test_user

def create_minimal_test_data(user_id):
    """Create minimal data to test table mode"""
    
    # Clear existing data
    PaymentData.query.filter_by(user_id=user_id).delete()
    IBRebate.query.filter_by(user_id=user_id).delete()
    CRMWithdrawals.query.filter_by(user_id=user_id).delete()
    CRMDeposit.query.filter_by(user_id=user_id).delete()
    AccountList.query.filter_by(user_id=user_id).delete()
    
    now = datetime.now()
    
    # Add minimal payment data (less than threshold)
    payment1 = PaymentData(
        user_id=user_id,
        tx_id='TEST001',
        status='DONE',
        type='DEPOSIT',
        sheet_category='M2p Deposit',
        final_amount=100.50,
        tier_fee=5.25,
        created=now
    )
    
    payment2 = PaymentData(
        user_id=user_id,
        tx_id='TEST002',
        status='DONE',
        type='WITHDRAW',
        sheet_category='M2p Withdraw',
        final_amount=50.00,
        tier_fee=2.50,
        created=now
    )
    
    # Add one rebate
    rebate1 = IBRebate(
        user_id=user_id,
        transaction_id='REBATE001',
        rebate=25.75,
        rebate_time=now
    )
    
    # Add one CRM deposit  
    crm_deposit1 = CRMDeposit(
        user_id=user_id,
        request_id='CRM001',
        trading_amount=75.25,
        payment_method='TOPCHANGE',
        request_time=now
    )
    
    db.session.add_all([payment1, payment2, rebate1, crm_deposit1])
    db.session.commit()
    print("Minimal test data created (should trigger table mode)")

def create_extensive_test_data(user_id):
    """Create extensive data to test chart mode"""
    
    # Clear existing data
    PaymentData.query.filter_by(user_id=user_id).delete()
    IBRebate.query.filter_by(user_id=user_id).delete()
    CRMWithdrawals.query.filter_by(user_id=user_id).delete()
    CRMDeposit.query.filter_by(user_id=user_id).delete()
    AccountList.query.filter_by(user_id=user_id).delete()
    
    now = datetime.now()
    
    # Create multiple payment records
    for i in range(10):
        # M2p Deposits
        payment = PaymentData(
            user_id=user_id,
            tx_id=f'M2P_DEP_{i}',
            status='DONE',
            type='DEPOSIT',
            sheet_category='M2p Deposit',
            final_amount=100 + i * 10,
            tier_fee=5 + i * 0.5,
            created=now - timedelta(days=i)
        )
        db.session.add(payment)
        
        # Settlement Deposits
        payment = PaymentData(
            user_id=user_id,
            tx_id=f'SETTLE_DEP_{i}',
            status='DONE',
            type='DEPOSIT',
            sheet_category='Settlement Deposit',
            final_amount=200 + i * 15,
            tier_fee=10 + i * 0.75,
            created=now - timedelta(days=i)
        )
        db.session.add(payment)
        
        # M2p Withdrawals
        payment = PaymentData(
            user_id=user_id,
            tx_id=f'M2P_WITH_{i}',
            status='DONE',
            type='WITHDRAW',
            sheet_category='M2p Withdraw',
            final_amount=50 + i * 5,
            tier_fee=2.5 + i * 0.25,
            created=now - timedelta(days=i)
        )
        db.session.add(payment)
        
        # Rebates
        rebate = IBRebate(
            user_id=user_id,
            transaction_id=f'REBATE_{i}',
            rebate=10 + i * 2,
            rebate_time=now - timedelta(days=i)
        )
        db.session.add(rebate)
        
        # CRM Deposits
        crm_deposit = CRMDeposit(
            user_id=user_id,
            request_id=f'CRM_DEP_{i}',
            trading_amount=150 + i * 12,
            payment_method='TOPCHANGE' if i % 3 == 0 else 'CARD',
            request_time=now - timedelta(days=i)
        )
        db.session.add(crm_deposit)
        
        # CRM Withdrawals
        crm_withdrawal = CRMWithdrawals(
            user_id=user_id,
            request_id=f'CRM_WITH_{i}',
            withdrawal_amount=80 + i * 8,
            trading_account=f'ACC{i}',
            review_time=now - timedelta(days=i)
        )
        db.session.add(crm_withdrawal)
    
    # Add welcome bonus accounts
    for i in range(3):
        account = AccountList(
            user_id=user_id,
            login=str(i),
            name=f'Welcome User {i}',
            group='WELCOME\\\\Welcome BBOOK',
            is_welcome_bonus=True
        )
        db.session.add(account)
    
    db.session.commit()
    print("Extensive test data created (should trigger chart mode)")

def test_data_sufficiency():
    """Test the data sufficiency checking"""
    print("\\n=== Testing Data Sufficiency ===")
    
    with app.app_context():
        test_user = create_test_user()
        
        # Test with minimal data
        create_minimal_test_data(test_user.id)
        with app.test_request_context():
            # Mock current_user
            from flask import g
            g._login_user = test_user
            
            check_result = check_data_sufficiency_for_charts()
            print(f"Minimal data check: {check_result}")
            
            report = generate_final_report()
            print(f"Report type (should be table): formatted_table={report.get('formatted_table', False)}")
            print(f"Report data sample: {report['report_data'][:3]}")
        
        # Test with extensive data
        create_extensive_test_data(test_user.id)
        with app.test_request_context():
            # Mock current_user  
            from flask import g
            g._login_user = test_user
            
            check_result = check_data_sufficiency_for_charts()
            print(f"\\nExtensive data check: {check_result}")
            
            report = generate_final_report()
            print(f"Report type (should be charts): formatted_table={report.get('formatted_table', False)}")
            print(f"Report calculations sample: {list(report['calculations'].keys())[:5]}")

if __name__ == '__main__':
    print("Testing Stage 2 Enhanced Functionality")
    test_data_sufficiency()
    print("\\nTest completed!")