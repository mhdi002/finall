import pandas as pd
from datetime import datetime
from sqlalchemy import and_, or_, func
from app.models import PaymentData, IBRebate, CRMWithdrawals, CRMDeposit, AccountList
from flask_login import current_user
import traceback
import re

def filter_by_date_range(query, start_date, end_date, date_column):
    """Apply date range filter to query"""
    if start_date and end_date:
        return query.filter(and_(date_column >= start_date, date_column <= end_date))
    return query

def _calculate_metrics(start_date=None, end_date=None):
    """Helper function to calculate all report metrics efficiently."""

    # Base queries
    payment_query = PaymentData.query.filter_by(user_id=current_user.id)
    rebate_query = IBRebate.query.filter_by(user_id=current_user.id)
    crm_withdraw_query = CRMWithdrawals.query.filter_by(user_id=current_user.id)
    crm_deposit_query = CRMDeposit.query.filter_by(user_id=current_user.id)

    # Apply date filters
    if start_date and end_date:
        payment_query = filter_by_date_range(payment_query, start_date, end_date, PaymentData.created)
        rebate_query = filter_by_date_range(rebate_query, start_date, end_date, IBRebate.rebate_time)
        crm_withdraw_query = filter_by_date_range(crm_withdraw_query, start_date, end_date, CRMWithdrawals.review_time)
        crm_deposit_query = filter_by_date_range(crm_deposit_query, start_date, end_date, CRMDeposit.request_time)

    # Efficiently calculate sums using the database
    def get_sum(query, column):
        return query.with_entities(func.sum(column)).scalar() or 0

    calculations = {
        'Total Rebate': get_sum(rebate_query, IBRebate.rebate),
        'M2p Deposit': get_sum(payment_query.filter_by(sheet_category='M2p Deposit'), PaymentData.final_amount),
        'Settlement Deposit': get_sum(payment_query.filter_by(sheet_category='Settlement Deposit'), PaymentData.final_amount),
        'M2p Withdrawal': get_sum(payment_query.filter_by(sheet_category='M2p Withdraw'), PaymentData.final_amount),
        'Settlement Withdrawal': get_sum(payment_query.filter_by(sheet_category='Settlement Withdraw'), PaymentData.final_amount),
        'CRM Deposit Total': get_sum(crm_deposit_query, CRMDeposit.trading_amount),
        'CRM Withdraw Total': get_sum(crm_withdraw_query, CRMWithdrawals.withdrawal_amount),
        'Tier Fee Deposit': (get_sum(payment_query.filter(PaymentData.sheet_category.ilike('%Deposit%')), PaymentData.tier_fee)),
        'Tier Fee Withdraw': (get_sum(payment_query.filter(PaymentData.sheet_category.ilike('%Withdraw%')), PaymentData.tier_fee)),
        'Topchange Deposit Total': get_sum(crm_deposit_query.filter(CRMDeposit.payment_method.ilike('TOPCHANGE')), CRMDeposit.trading_amount),
    }

    # Welcome bonus calculation still requires some Python logic
    welcome_logins = db.session.query(AccountList.login).filter_by(user_id=current_user.id, is_welcome_bonus=True).all()
    welcome_logins = [login[0] for login in welcome_logins]

    welcome_bonus_withdrawals = 0
    if welcome_logins:
        # This part is still tricky to do in pure SQL with the current schema
        withdrawals = crm_withdraw_query.all()
        for w in withdrawals:
            match = re.search(r'\d+', str(w.trading_account or ''))
            if match and match.group(0) in welcome_logins:
                welcome_bonus_withdrawals += w.withdrawal_amount or 0
    calculations['Welcome Bonus Withdrawals'] = welcome_bonus_withdrawals

    return calculations

def check_data_sufficiency_for_charts(start_date=None, end_date=None):
    """
    Check if there's sufficient data for meaningful chart generation
    Returns True if charts should be shown, False if table should be shown instead
    """
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
    
    # Count total records
    payment_count = payment_query.count()
    rebate_count = rebate_query.count()
    crm_withdraw_count = crm_withdraw_query.count()
    crm_deposit_count = crm_deposit_query.count()
    
    total_records = payment_count + rebate_count + crm_withdraw_count + crm_deposit_count
    
    # Define thresholds for meaningful charts
    MIN_RECORDS_FOR_CHARTS = 20  # Minimum total records
    MIN_CATEGORIES_WITH_DATA = 3  # At least 3 categories should have data
    
    # Count categories with data
    categories_with_data = 0
    if payment_count > 0:
        categories_with_data += 1
    if rebate_count > 0:
        categories_with_data += 1
    if crm_withdraw_count > 0:
        categories_with_data += 1
    if crm_deposit_count > 0:
        categories_with_data += 1
    
    # Check if data is sufficient for charts
    sufficient_data = (total_records >= MIN_RECORDS_FOR_CHARTS and 
                      categories_with_data >= MIN_CATEGORIES_WITH_DATA)
    
    return {
        'sufficient_for_charts': sufficient_data,
        'total_records': total_records,
        'categories_with_data': categories_with_data,
        'breakdown': {
            'payments': payment_count,
            'rebates': rebate_count,
            'crm_withdrawals': crm_withdraw_count,
            'crm_deposits': crm_deposit_count
        }
    }

def generate_formatted_final_report(start_date=None, end_date=None):
    """
    Generate final report similar to the Google Apps Script version
    This is shown when data is insufficient for charts
    """
    calculations = _calculate_metrics(start_date, end_date)
    
    metrics_order = [
        'Total Rebate',
        'M2p Deposit',
        'Settlement Deposit',
        'M2p Withdrawal',
        'Settlement Withdrawal',
        'CRM Deposit Total',
        'Topchange Deposit Total',
        'Tier Fee Deposit',
        'Tier Fee Withdraw',
        'Welcome Bonus Withdrawals',
        'CRM Withdraw Total'
    ]
    
    report_data = []
    date_range_str = ''
    
    if start_date and end_date:
        date_range_str = f"Filtered from {start_date.strftime('%d.%m.%Y')} to {end_date.strftime('%d.%m.%Y')}"
        report_data.append(['Date Range', date_range_str])
        report_data.append(['', ''])
    
    for metric in metrics_order:
        value = calculations.get(metric, 0)
        report_data.append([metric, f"{value:.2f}"])
    
    return {
        'report_data': report_data,
        'calculations': calculations,
        'date_range': date_range_str,
        'formatted_table': True
    }

def generate_original_final_report(start_date=None, end_date=None):
    """Original final report generation for cases with sufficient data"""
    calculations = _calculate_metrics(start_date, end_date)
    
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
        'date_range': date_range_str,
        'formatted_table': False
    }

def generate_final_report(start_date=None, end_date=None):
    """
    Enhanced version of the original generate_final_report that checks data sufficiency
    """
    data_check = check_data_sufficiency_for_charts(start_date, end_date)

    if data_check['sufficient_for_charts']:
        return generate_original_final_report(start_date, end_date)
    else:
        return generate_formatted_final_report(start_date, end_date)

def compare_crm_and_client_deposits(start_date=None, end_date=None):
    """Compare CRM deposits with client payment deposits to find discrepancies"""
    
    crm_query = CRMDeposit.query.filter_by(user_id=current_user.id)
    client_query = PaymentData.query.filter_by(user_id=current_user.id, sheet_category='M2p Deposit')
    
    if start_date and end_date:
        crm_query = filter_by_date_range(crm_query, start_date, end_date, CRMDeposit.request_time)
        client_query = filter_by_date_range(client_query, start_date, end_date, PaymentData.created)
    
    crm_deposits = crm_query.all()
    client_deposits = client_query.all()
    
    crm_normalized = [
        {
            'date': d.request_time, 'client_id': (d.client_id or '').strip().lower(),
            'name': d.name or '', 'amount': float(d.trading_amount or 0),
            'payment_method': (d.payment_method or '').strip().lower(),
            'source': 'CRM Deposit', 'id': d.id
        } for d in crm_deposits
    ]
    
    client_normalized = [
        {
            'date': d.created, 'account': (d.trading_account or '').strip().lower(),
            'amount': float(d.final_amount or 0), 'source': 'M2p Deposit', 'id': d.id
        } for d in client_deposits
    ]
    
    matched = set()
    unmatched = []
    
    for crm_row in crm_normalized:
        match_found = False
        for client_row in client_normalized:
            if client_row['id'] in matched:
                continue
            
            if crm_row['date'] and client_row['date']:
                time_diff = abs((crm_row['date'] - client_row['date']).total_seconds())
                if time_diff <= 3.5 * 3600 and \
                   crm_row['client_id'] in client_row['account'] and \
                   abs(crm_row['amount'] - client_row['amount']) <= 1:
                    matched.add(client_row['id'])
                    match_found = True
                    break
        
        if not match_found and crm_row['payment_method'] != 'topchange':
            unmatched.append([
                crm_row['source'], crm_row['date'].strftime('%Y-%m-%d') if crm_row['date'] else '',
                crm_row['client_id'], '', f"{crm_row['amount']:.2f}", crm_row['name'], 'N', crm_row['id']
            ])

    for client_row in client_normalized:
        if client_row['id'] not in matched:
            unmatched.append([
                client_row['source'], client_row['date'].strftime('%Y-%m-%d') if client_row['date'] else '',
                '', client_row['account'], f"{client_row['amount']:.2f}", '', 'N', client_row['id']
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
    """Get summary data for creating charts - only when data is sufficient"""
    data_check = check_data_sufficiency_for_charts(start_date, end_date)
    
    if not data_check['sufficient_for_charts']:
        return None
    
    report = generate_original_final_report(start_date, end_date)
    calculations = report['calculations']
    
    volumes = {
        'M2p Deposit': calculations.get('M2p Deposit', 0),
        'Settlement Deposit': calculations.get('Settlement Deposit', 0),
        'M2p Withdrawal': calculations.get('M2p Withdrawal', 0),
        'Settlement Withdrawal': calculations.get('Settlement Withdrawal', 0),
        'CRM Deposit': calculations.get('CRM Deposit Total', 0),
        'CRM Withdrawal': calculations.get('CRM Withdraw Total', 0)
    }
    
    fees = {
        'Tier Fee Deposit': calculations.get('Tier Fee Deposit', 0),
        'Tier Fee Withdraw': calculations.get('Tier Fee Withdraw', 0),
        'Total Rebate': calculations.get('Total Rebate', 0)
    }
    
    return {
        'volumes': volumes,
        'fees': fees,
        'calculations': calculations,
        'data_sufficiency': data_check
    }