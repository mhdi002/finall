#!/usr/bin/env python
"""
Setup test data for the demo user to demonstrate both table and chart modes
"""
from app import create_app, db
from app.models import User, PaymentData, IBRebate, CRMWithdrawals, CRMDeposit, AccountList
from datetime import datetime, timedelta

app = create_app()

def setup_demo_data():
    with app.app_context():
        # Get demo user
        demo_user = User.query.filter_by(username='demo').first()
        if not demo_user:
            print("❌ Demo user not found")
            return
        
        print(f"✅ Found demo user: {demo_user.username}")
        
        # Clear existing data
        PaymentData.query.filter_by(user_id=demo_user.id).delete()
        IBRebate.query.filter_by(user_id=demo_user.id).delete()
        CRMWithdrawals.query.filter_by(user_id=demo_user.id).delete()
        CRMDeposit.query.filter_by(user_id=demo_user.id).delete()
        AccountList.query.filter_by(user_id=demo_user.id).delete()
        
        # Create moderate amount of data (should trigger chart mode)
        now = datetime.now()
        
        # Add payment data (15 records across categories)
        payment_data = [
            # M2p Deposits
            ('M2P_DEP_001', 'M2p Deposit', 'DEPOSIT', 500.25, 25.50),
            ('M2P_DEP_002', 'M2p Deposit', 'DEPOSIT', 750.00, 35.75),
            ('M2P_DEP_003', 'M2p Deposit', 'DEPOSIT', 1200.50, 60.25),
            ('M2P_DEP_004', 'M2p Deposit', 'DEPOSIT', 890.75, 44.50),
            ('M2P_DEP_005', 'M2p Deposit', 'DEPOSIT', 650.00, 32.50),
            
            # Settlement Deposits
            ('SET_DEP_001', 'Settlement Deposit', 'DEPOSIT', 1500.00, 75.00),
            ('SET_DEP_002', 'Settlement Deposit', 'DEPOSIT', 2200.25, 110.10),
            ('SET_DEP_003', 'Settlement Deposit', 'DEPOSIT', 980.50, 49.25),
            
            # M2p Withdrawals
            ('M2P_WITH_001', 'M2p Withdraw', 'WITHDRAW', 300.00, 15.00),
            ('M2P_WITH_002', 'M2p Withdraw', 'WITHDRAW', 450.75, 22.50),
            ('M2P_WITH_003', 'M2p Withdraw', 'WITHDRAW', 680.25, 34.00),
            
            # Settlement Withdrawals
            ('SET_WITH_001', 'Settlement Withdraw', 'WITHDRAW', 800.00, 40.00),
            ('SET_WITH_002', 'Settlement Withdraw', 'WITHDRAW', 1150.50, 57.75),
        ]
        
        for i, (tx_id, category, tx_type, amount, fee) in enumerate(payment_data):
            payment = PaymentData(
                user_id=demo_user.id,
                tx_id=tx_id,
                status='DONE',
                type=tx_type,
                sheet_category=category,
                final_amount=amount,
                tier_fee=fee,
                created=now - timedelta(days=i)
            )
            db.session.add(payment)
        
        # Add rebate data (8 records)
        rebate_data = [125.50, 89.75, 156.25, 203.00, 178.50, 95.25, 134.75, 167.00]
        for i, rebate_amount in enumerate(rebate_data):
            rebate = IBRebate(
                user_id=demo_user.id,
                transaction_id=f'REBATE_{i+1:03d}',
                rebate=rebate_amount,
                rebate_time=now - timedelta(days=i)
            )
            db.session.add(rebate)
        
        # Add CRM deposit data (12 records)
        crm_deposit_data = [
            (850.00, 'CARD'), (1250.75, 'TOPCHANGE'), (950.25, 'CARD'),
            (1560.00, 'TOPCHANGE'), (720.50, 'CARD'), (1890.75, 'CARD'),
            (675.25, 'TOPCHANGE'), (1125.00, 'CARD'), (2100.50, 'CARD'),
            (840.75, 'TOPCHANGE'), (1375.25, 'CARD'), (995.00, 'CARD')
        ]
        
        for i, (amount, method) in enumerate(crm_deposit_data):
            crm_deposit = CRMDeposit(
                user_id=demo_user.id,
                request_id=f'CRM_DEP_{i+1:03d}',
                trading_amount=amount,
                payment_method=method,
                client_id=f'CLIENT_{i+1000}',
                name=f'Client Name {i+1}',
                request_time=now - timedelta(days=i)
            )
            db.session.add(crm_deposit)
        
        # Add CRM withdrawal data (10 records)
        crm_withdrawal_data = [450.00, 670.25, 890.50, 1120.75, 340.00, 780.25, 950.50, 1250.00, 580.75, 825.25]
        for i, amount in enumerate(crm_withdrawal_data):
            crm_withdrawal = CRMWithdrawals(
                user_id=demo_user.id,
                request_id=f'CRM_WITH_{i+1:03d}',
                withdrawal_amount=amount,
                trading_account=f'ACCOUNT_{i+1000}',
                review_time=now - timedelta(days=i)
            )
            db.session.add(crm_withdrawal)
        
        # Add welcome bonus accounts (3 records)
        for i in range(3):
            account = AccountList(
                user_id=demo_user.id,
                login=str(1000 + i),
                name=f'Welcome Bonus User {i+1}',
                group='WELCOME\\\\Welcome BBOOK',
                is_welcome_bonus=True
            )
            db.session.add(account)
        
        db.session.commit()
        
        # Count totals
        total_payments = PaymentData.query.filter_by(user_id=demo_user.id).count()
        total_rebates = IBRebate.query.filter_by(user_id=demo_user.id).count()
        total_crm_deposits = CRMDeposit.query.filter_by(user_id=demo_user.id).count()
        total_crm_withdrawals = CRMWithdrawals.query.filter_by(user_id=demo_user.id).count()
        total_accounts = AccountList.query.filter_by(user_id=demo_user.id).count()
        
        total_records = total_payments + total_rebates + total_crm_deposits + total_crm_withdrawals
        
        print(f"✅ Demo data created successfully:")
        print(f"   📊 Payment records: {total_payments}")
        print(f"   💰 Rebate records: {total_rebates}")
        print(f"   📈 CRM Deposit records: {total_crm_deposits}")
        print(f"   📉 CRM Withdrawal records: {total_crm_withdrawals}")
        print(f"   👥 Account records: {total_accounts}")
        print(f"   🎯 Total records: {total_records}")
        print(f"   🖥️  Expected mode: {'CHART MODE' if total_records >= 20 else 'TABLE MODE'}")

if __name__ == '__main__':
    print("🚀 Setting up demo data...")
    setup_demo_data()
    print("✅ Setup complete!")