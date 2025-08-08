import pandas as pd
from datetime import datetime
from sqlalchemy import and_, or_
from app.models import PaymentData, IBRebate, CRMWithdrawals, CRMDeposit, AccountList
from flask_login import current_user

def filter_by_date_range(query, start_date, end_date, date_column):
    """Apply date range filter to query"""
    if start_date and end_date:
        return query.filter(and_(date_column >= start_date, date_column <= end_date))
    return query

def sum_column_from_query(query, column_name):
    """Sum a column from a query result"""
    try:
        total = 0
        for record in query:
            value = getattr(record, column_name, 0)
            if value:
                total += float(value)
        return total
    except:
        return 0

def generate_final_report(start_date=None, end_date=None):
    """Generate comprehensive final report"""
    
    # Base queries for current user
    payment_query = PaymentData.query.filter_by(user_id=current_user.id)
    rebate_query = IBRebate.query.filter_by(user_id=current_user.id)
    crm_withdraw_query = CRMWithdrawals.query.filter_by(user_id=current_user.id)
    crm_deposit_query = CRMDeposit.query.filter_by(user_id=current_user.id)
    
    # Apply date filters if provided
    if start_date and end_date:
        payment_query = filter_by_date_range(payment_query, start_date, end_date, PaymentData.created)
        rebate_query = filter_by_date_range(rebate_query, start_date, end_date, IBRebate.rebate_time)
        crm_withdraw_query = filter_by_date_range(crm_withdraw_query, start_date, end_date, CRMWithdrawals.review_time)
        crm_deposit_query = filter_by_date_range(crm_deposit_query, start_date, end_date, CRMDeposit.request_time)
    
    # Calculate totals
    calculations = {}
    
    # 1. Total Rebate
    calculations['Total Rebate'] = sum_column_from_query(rebate_query.all(), 'rebate')
    
    # 2. Deposits by category
    m2p_deposits = payment_query.filter_by(sheet_category='M2p Deposit').all()
    settlement_deposits = payment_query.filter_by(sheet_category='Settlement Deposit').all()
    
    calculations['M2p Deposit'] = sum_column_from_query(m2p_deposits, 'final_amount')
    calculations['Settlement Deposit'] = sum_column_from_query(settlement_deposits, 'final_amount')
    
    # 3. Withdrawals by category
    m2p_withdraws = payment_query.filter_by(sheet_category='M2p Withdraw').all()
    settlement_withdraws = payment_query.filter_by(sheet_category='Settlement Withdraw').all()
    
    calculations['M2p Withdrawal'] = sum_column_from_query(m2p_withdraws, 'final_amount')
    calculations['Settlement Withdrawal'] = sum_column_from_query(settlement_withdraws, 'final_amount')
    
    # 4. CRM Deposit Total
    calculations['CRM Deposit Total'] = sum_column_from_query(crm_deposit_query.all(), 'trading_amount')
    
    # 5. Tier Fees
    tier_fee_deposit = (sum_column_from_query(m2p_deposits, 'tier_fee') + 
                       sum_column_from_query(settlement_deposits, 'tier_fee'))
    tier_fee_withdraw = (sum_column_from_query(m2p_withdraws, 'tier_fee') + 
                        sum_column_from_query(settlement_withdraws, 'tier_fee'))
    
    calculations['Tier Fee Deposit'] = tier_fee_deposit
    calculations['Tier Fee Withdraw'] = tier_fee_withdraw
    
    # 6. Welcome Bonus Withdrawals
    welcome_accounts = AccountList.query.filter_by(user_id=current_user.id, is_welcome_bonus=True).all()
    welcome_logins = [acc.login for acc in welcome_accounts]
    
    welcome_withdraw_sum = 0
    if welcome_logins:
        for withdrawal in crm_withdraw_query.all():
            # Extract login number from trading account
            trading_account = str(withdrawal.trading_account or '')
            login_match = None
            import re
            match = re.search(r'\d+', trading_account)
            if match:
                login_match = match.group()
            
            if login_match and login_match in welcome_logins:
                welcome_withdraw_sum += float(withdrawal.withdrawal_amount or 0)
    
    calculations['Welcome Bonus Withdrawals'] = welcome_withdraw_sum
    
    # 7. CRM TopChange Total
    topchange_total = 0
    for deposit in crm_deposit_query.all():
        if (deposit.payment_method or '').upper() == 'TOPCHANGE':
            topchange_total += float(deposit.trading_amount or 0)
    
    calculations['CRM TopChange Total'] = topchange_total
    
    # 8. CRM Withdraw Total
    calculations['CRM Withdraw Total'] = sum_column_from_query(crm_withdraw_query.all(), 'withdrawal_amount')
    
    # Format as list of tuples for display
    report_data = []
    date_range_str = ''
    if start_date and end_date:
        date_range_str = f"From {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        report_data.append(['Date Range', date_range_str])
        report_data.append(['', ''])
    
    for key, value in calculations.items():
        report_data.append([key, f"{value:.2f}"])
    
    return {
        'report_data': report_data,
        'calculations': calculations,
        'date_range': date_range_str
    }

def compare_crm_and_client_deposits(start_date=None, end_date=None):
    """Compare CRM deposits with client payment deposits to find discrepancies"""
    
    # Get CRM deposits
    crm_query = CRMDeposit.query.filter_by(user_id=current_user.id)
    client_query = PaymentData.query.filter_by(user_id=current_user.id, sheet_category='M2p Deposit')
    
    # Apply date filters
    if start_date and end_date:
        crm_query = filter_by_date_range(crm_query, start_date, end_date, CRMDeposit.request_time)
        client_query = filter_by_date_range(client_query, start_date, end_date, PaymentData.created)
    
    crm_deposits = crm_query.all()
    client_deposits = client_query.all()
    
    # Normalize data for comparison
    crm_normalized = []
    for deposit in crm_deposits:
        crm_normalized.append({
            'date': deposit.request_time,
            'client_id': (deposit.client_id or '').strip().lower(),
            'name': deposit.name or '',
            'amount': float(deposit.trading_amount or 0),
            'payment_method': (deposit.payment_method or '').strip().lower(),
            'source': 'CRM Deposit',
            'id': deposit.id
        })
    
    client_normalized = []
    for deposit in client_deposits:
        client_normalized.append({
            'date': deposit.created,
            'account': (deposit.trading_account or '').strip().lower(),
            'amount': float(deposit.final_amount or 0),
            'source': 'M2p Deposit',
            'id': deposit.id
        })
    
    # Find matches and discrepancies
    matched = set()
    unmatched = []
    
    # Compare CRM with Client deposits
    for crm_row in crm_normalized:
        match_found = False
        for client_row in client_normalized:
            if client_row['id'] in matched:
                continue
            
            # Check if dates are within 3.5 hours of each other
            if crm_row['date'] and client_row['date']:
                time_diff = abs((crm_row['date'] - client_row['date']).total_seconds())
                if time_diff <= 3.5 * 3600:  # 3.5 hours
                    # Check if client ID is in trading account
                    if crm_row['client_id'] in client_row['account']:
                        # Check if amounts are similar (within $1)
                        if abs(crm_row['amount'] - client_row['amount']) <= 1:
                            matched.add(client_row['id'])
                            match_found = True
                            break
        
        # If no match found and not TopChange, add to unmatched
        if not match_found and crm_row['payment_method'] != 'topchange':
            unmatched.append([
                crm_row['source'],
                crm_row['date'].strftime('%Y-%m-%d') if crm_row['date'] else '',
                crm_row['client_id'],
                '',
                f"{crm_row['amount']:.2f}",
                crm_row['name'],
                'N',  # Confirmed status
                crm_row['id']
            ])
    
    # Add unmatched client deposits
    for client_row in client_normalized:
        if client_row['id'] not in matched:
            unmatched.append([
                client_row['source'],
                client_row['date'].strftime('%Y-%m-%d') if client_row['date'] else '',
                '',  # Client ID
                client_row['account'],
                f"{client_row['amount']:.2f}",
                '',  # Client Name
                'N',  # Confirmed status
                client_row['id']
            ])
    
    headers = ['Source', 'Date', 'Client ID', 'Trading Account', 'Amount', 'Client Name', 'Confirmed (Y/N)', 'ID']
    
    return {
        'headers': headers,
        'discrepancies': unmatched,
        'total_discrepancies': len(unmatched)
    }

def get_payment_data_by_category(category, start_date=None, end_date=None):
    """Get payment data filtered by category and optionally by date range"""
    query = PaymentData.query.filter_by(user_id=current_user.id, sheet_category=category)
    
    if start_date and end_date:
        query = filter_by_date_range(query, start_date, end_date, PaymentData.created)
    
    return query.all()

def get_summary_data_for_charts(start_date=None, end_date=None):
    """Get summary data for creating charts"""
    report = generate_final_report(start_date, end_date)
    calculations = report['calculations']
    
    # Volume data
    volumes = {
        'M2p Deposit': calculations.get('M2p Deposit', 0),
        'Settlement Deposit': calculations.get('Settlement Deposit', 0),
        'M2p Withdrawal': calculations.get('M2p Withdrawal', 0),
        'Settlement Withdrawal': calculations.get('Settlement Withdrawal', 0),
        'CRM Deposit': calculations.get('CRM Deposit Total', 0),
        'CRM Withdrawal': calculations.get('CRM Withdraw Total', 0)
    }
    
    # Fee data
    fees = {
        'Tier Fee Deposit': calculations.get('Tier Fee Deposit', 0),
        'Tier Fee Withdraw': calculations.get('Tier Fee Withdraw', 0),
        'Total Rebate': calculations.get('Total Rebate', 0)
    }
    
    return {
        'volumes': volumes,
        'fees': fees,
        'calculations': calculations
    }