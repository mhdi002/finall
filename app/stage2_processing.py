import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from app import db
from app.models import PaymentData, IBRebate, CRMWithdrawals, CRMDeposit, AccountList, UploadedFiles
from flask_login import current_user
import uuid
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_float(value, default=0.0):
    """Safely convert a value to a float."""
    if value is None:
        return default
    try:
        # Remove currency symbols, commas, etc.
        cleaned_value = re.sub(r'[^\d.-]', '', str(value))
        if cleaned_value:
            return float(cleaned_value)
    except (ValueError, TypeError):
        pass
    return default

def safe_str(value, default=''):
    """Safely convert a value to a string."""
    if value is None:
        return default
    try:
        return str(value)
    except (ValueError, TypeError):
        return default

def detect_separator(line):
    """Detect CSV separator based on character count"""
    tab_count = line.count('\t')
    comma_count = line.count(',')
    semicolon_count = line.count(';')
    
    logger.info(f"Separator detection - Tab: {tab_count}, Comma: {comma_count}, Semicolon: {semicolon_count}")
    
    if tab_count >= comma_count and tab_count >= semicolon_count:
        return '\t'
    elif semicolon_count >= comma_count:
        return ';'
    return ','

def parse_date_flexible(date_str):
    """Parse dates in various formats"""
    if pd.isna(date_str) or not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Try different date formats
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%d.%m.%Y %H:%M:%S',
        '%d.%m.%Y',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def find_column_index(headers, search_terms, exact_match=None):
    """
    Find column index by searching for terms in headers
    Args:
        headers: List of column headers
        search_terms: List of terms to search for (case insensitive)
        exact_match: If provided, look for exact match first
    """
    # First try exact match if provided
    if exact_match:
        for i, header in enumerate(headers):
            if header.strip().upper() == exact_match.upper():
                logger.info(f"Found exact match '{exact_match}' at index {i}")
                return i
    
    # Then try partial matches
    for i, header in enumerate(headers):
        header_upper = header.strip().upper()
        for term in search_terms:
            if term.upper() in header_upper:
                logger.info(f"Found '{term}' in '{header}' at index {i}")
                return i
    
    return None

def read_file_with_encoding(file_path, file_format='csv'):
    """Read file with proper encoding detection"""
    if file_format.lower() == 'xlsx':
        logger.info("Reading XLSX file")
        return pd.read_excel(file_path)
    
    # Try different encodings for CSV
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            logger.info(f"Trying encoding: {encoding}")
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline()
                separator = detect_separator(first_line)
            
            data = pd.read_csv(file_path, sep=separator, encoding=encoding)
            logger.info(f"Successfully read file with {encoding} encoding and '{separator}' separator")
            return data
            
        except (UnicodeDecodeError, pd.errors.EmptyDataError) as e:
            logger.warning(f"Failed to read with {encoding}: {e}")
            continue
    
    raise ValueError("Could not read file with any supported encoding")

def filter_unique_rows(existing_keys, new_rows, key_columns, data_headers):
    """Filter out duplicate rows based on key columns"""
    unique_rows = []
    
    for row in new_rows:
        # Create key from specified columns
        key_parts = []
        for idx in key_columns:
            if idx < len(row):
                val = str(row[idx] or '').strip().upper()
                key_parts.append(val)
        
        key = '|'.join(key_parts)
        
        if key and key not in existing_keys:
            existing_keys.add(key)
            unique_rows.append(row)
    
    return unique_rows

def process_payment_data(file_path, file_format='csv'):
    """Process payment CSV/XLSX data and store in database"""
    logger.info(f"Processing payment data from: {file_path}")
    
    try:
        data = read_file_with_encoding(file_path, file_format)
        
        if data.empty or len(data) < 1:
            raise ValueError("File is empty or invalid")
        
        headers = data.columns.tolist()
        rows = data.values.tolist()
        
        logger.info(f"File has {len(rows)} rows and {len(headers)} columns")
        logger.info(f"Headers: {headers}")
        
        # Define column mapping
        column_map = {
            'confirmed': 'Confirmed',
            'tx_id': 'Transaction ID', 
            'wallet_address': 'Wallet address',
            'status': 'Status',
            'type': 'Type',
            'payment_gateway': 'Payment gateway',
            'final_amount': 'Transaction amount',
            'final_currency': 'Transaction currency',
            'settlement_amount': 'Settlement amount',
            'settlement_currency': 'Settlement currency',
            'processing_fee': 'Processing fee',
            'price': 'Price',
            'comment': 'Comment',
            'payment_id': 'Payment ID',
            'created': 'Booked',
            'trading_account': 'Trading account',
            'balance_after': 'Balance after',
            'tier_fee': 'Tier fee'
        }
        
        added_count = 0
        skipped_count = 0
        
        for i, row in enumerate(rows):
            try:
                # Create row dictionary
                row_dict = {}
                for j, header in enumerate(headers):
                    if j < len(row):
                        row_dict[header.strip()] = row[j]
                
                # Extract values
                tx_id = str(row_dict.get(column_map.get('tx_id', ''), '')).strip()
                status = str(row_dict.get(column_map.get('status', ''), '')).upper()
                pg_name = str(row_dict.get(column_map.get('payment_gateway', ''), '')).upper()
                tx_type = str(row_dict.get(column_map.get('type', ''), '')).upper()
                
                logger.info(f"Row {i+1}: tx_id='{tx_id}', status='{status}', pg_name='{pg_name}', type='{tx_type}'")
                
                if not tx_id:
                    logger.warning(f"Row {i+1}: Skipped - No transaction ID")
                    skipped_count += 1
                    continue
                
                if pg_name == 'BALANCE':
                    logger.info(f"Row {i+1}: Skipped - Payment gateway is BALANCE")
                    skipped_count += 1
                    continue
                
                if status != 'DONE':
                    logger.info(f"Row {i+1}: Skipped - Status is not DONE (status: {status})")
                    skipped_count += 1
                    continue
                
                # Check if already exists
                existing = PaymentData.query.filter_by(tx_id=tx_id).first()
                if existing:
                    logger.info(f"Row {i+1}: Skipped - Transaction ID {tx_id} already exists in database")
                    skipped_count += 1
                    continue
                
                # Determine sheet category
                sheet_category = ''
                if tx_type == 'DEPOSIT':
                    sheet_category = 'Settlement Deposit' if 'SETTLEMENT' in pg_name else 'M2p Deposit'
                else:
                    sheet_category = 'Settlement Withdraw' if 'SETTLEMENT' in pg_name else 'M2p Withdraw'
                
                # Create new payment record
                payment = PaymentData(
                    user_id=current_user.id,
                    confirmed=safe_str(row_dict.get(column_map.get('confirmed', ''))),
                    tx_id=tx_id,
                    wallet_address=safe_str(row_dict.get(column_map.get('wallet_address', ''))),
                    status=status,
                    type=tx_type,
                    payment_gateway=safe_str(row_dict.get(column_map.get('payment_gateway', ''))),
                    final_amount=safe_float(row_dict.get(column_map.get('final_amount', ''))),
                    final_currency=safe_str(row_dict.get(column_map.get('final_currency', ''))),
                    settlement_amount=safe_float(row_dict.get(column_map.get('settlement_amount', ''))),
                    settlement_currency=safe_str(row_dict.get(column_map.get('settlement_currency', ''))),
                    processing_fee=safe_float(row_dict.get(column_map.get('processing_fee', ''))),
                    price=safe_float(row_dict.get(column_map.get('price', '')), default=1.0),
                    comment=safe_str(row_dict.get(column_map.get('comment', ''))),
                    payment_id=safe_str(row_dict.get(column_map.get('payment_id', ''))),
                    created=parse_date_flexible(row_dict.get(column_map.get('created', ''))),
                    trading_account=safe_str(row_dict.get(column_map.get('trading_account', ''))),
                    correct_coin_sent=True,
                    balance_after=safe_float(row_dict.get(column_map.get('balance_after', ''))),
                    tier_fee=safe_float(row_dict.get(column_map.get('tier_fee', ''))),
                    sheet_category=sheet_category
                )
                
                db.session.add(payment)
                logger.info(f"Row {i+1}: Added payment record for transaction {tx_id}")
                added_count += 1
                
            except Exception as e:
                logger.error(f"Row {i+1}: Error processing - {e}")
                skipped_count += 1
                continue
        
        db.session.commit()
        logger.info(f"Processing complete: {added_count} added, {skipped_count} skipped")
        return {'added_rows': added_count, 'total_rows': len(rows), 'skipped_rows': skipped_count}
        
    except Exception as e:
        logger.error(f"Fatal error processing payment data: {e}")
        db.session.rollback()
        raise e

def process_ib_rebate(file_path, file_format='csv'):
    """Process IB Rebate CSV/XLSX data"""
    logger.info(f"Processing IB rebate data from: {file_path}")
    
    try:
        data = read_file_with_encoding(file_path, file_format)
        
        if data.empty:
            raise ValueError("File is empty or invalid")
        
        headers = data.columns.tolist()
        rows = data.values.tolist()
        
        logger.info(f"File has {len(rows)} rows and {len(headers)} columns")
        logger.info(f"Headers: {headers}")
        
        # Find required columns with improved logic
        tx_id_idx = find_column_index(headers, ['Transaction ID', 'TRANSACTION_ID'], 'Transaction ID')
        rebate_idx = find_column_index(headers, ['Rebate'], 'Rebate')
        rebate_time_idx = find_column_index(headers, ['Rebate Time', 'REBATE_TIME'], 'Rebate Time')
        
        logger.info(f"Column indices - tx_id: {tx_id_idx}, rebate: {rebate_idx}, rebate_time: {rebate_time_idx}")
        
        if tx_id_idx is None:
            raise ValueError("Transaction ID column not found")
        if rebate_time_idx is None:
            raise ValueError("Rebate Time column not found")
        
        added_count = 0
        skipped_count = 0
        
        for i, row in enumerate(rows):
            try:
                if len(row) <= tx_id_idx:
                    logger.warning(f"Row {i+1}: Skipped - insufficient columns ({len(row)} <= {tx_id_idx})")
                    skipped_count += 1
                    continue
                
                tx_id = str(row[tx_id_idx] or '').strip()
                if not tx_id:
                    logger.warning(f"Row {i+1}: Skipped - empty transaction ID")
                    skipped_count += 1
                    continue
                
                logger.info(f"Row {i+1}: Processing transaction ID '{tx_id}'")
                
                # Check if already exists
                existing = IBRebate.query.filter_by(transaction_id=tx_id).first()
                if existing:
                    logger.info(f"Row {i+1}: Skipped - transaction ID {tx_id} already exists in database")
                    skipped_count += 1
                    continue
                
                rebate_value = safe_float(row[rebate_idx]) if rebate_idx is not None and rebate_idx < len(row) else 0.0
                rebate_time = parse_date_flexible(row[rebate_time_idx]) if rebate_time_idx is not None and rebate_time_idx < len(row) else None
                
                logger.info(f"Row {i+1}: rebate_value={rebate_value}, rebate_time={rebate_time}")
                
                rebate = IBRebate(
                    user_id=current_user.id,
                    transaction_id=safe_str(tx_id),
                    rebate=rebate_value,
                    rebate_time=rebate_time
                )
                
                db.session.add(rebate)
                logger.info(f"Row {i+1}: Added rebate record for transaction {tx_id}")
                added_count += 1
                
            except Exception as e:
                logger.error(f"Row {i+1}: Error processing - {e}")
                skipped_count += 1
                continue
        
        db.session.commit()
        logger.info(f"Processing complete: {added_count} added, {skipped_count} skipped")
        return {'added_rows': added_count, 'total_rows': len(rows), 'skipped_rows': skipped_count}
        
    except Exception as e:
        logger.error(f"Fatal error processing IB rebate data: {e}")
        db.session.rollback()
        raise e

def process_crm_withdrawals(file_path, file_format='csv'):
    """Process CRM Withdrawals CSV/XLSX data"""
    logger.info(f"Processing CRM withdrawals from: {file_path}")
    
    try:
        data = read_file_with_encoding(file_path, file_format)
        
        if data.empty:
            raise ValueError("File is empty or invalid")
        
        headers = data.columns.tolist()
        rows = data.values.tolist()
        
        logger.info(f"File has {len(rows)} rows and {len(headers)} columns")
        logger.info(f"Headers: {headers}")
        
        # Find required columns
        req_time_idx = find_column_index(headers, ['Review Time', 'REVIEW_TIME'])
        trading_account_idx = find_column_index(headers, ['Trading Account', 'TRADING_ACCOUNT'])
        amount_idx = find_column_index(headers, ['Withdrawal Amount', 'WITHDRAWAL_AMOUNT'])
        request_id_idx = find_column_index(headers, ['Request ID', 'REQUEST_ID'])
        
        logger.info(f"Column indices - req_time: {req_time_idx}, trading_account: {trading_account_idx}, amount: {amount_idx}, request_id: {request_id_idx}")
        
        if None in [req_time_idx, trading_account_idx, amount_idx, request_id_idx]:
            missing = []
            if req_time_idx is None: missing.append("Review Time")
            if trading_account_idx is None: missing.append("Trading Account")
            if amount_idx is None: missing.append("Withdrawal Amount")
            if request_id_idx is None: missing.append("Request ID")
            raise ValueError(f"Required columns not found: {', '.join(missing)}")
        
        added_count = 0
        skipped_count = 0
        
        for i, row in enumerate(rows):
            try:
                max_idx = max(req_time_idx, trading_account_idx, amount_idx, request_id_idx)
                if len(row) <= max_idx:
                    logger.warning(f"Row {i+1}: Skipped - insufficient columns ({len(row)} <= {max_idx})")
                    skipped_count += 1
                    continue
                
                request_id = str(row[request_id_idx] or '').strip()
                if not request_id:
                    logger.warning(f"Row {i+1}: Skipped - empty request ID")
                    skipped_count += 1
                    continue
                
                logger.info(f"Row {i+1}: Processing request ID '{request_id}'")
                
                # Check if already exists
                existing = CRMWithdrawals.query.filter_by(request_id=request_id).first()
                if existing:
                    logger.info(f"Row {i+1}: Skipped - request ID {request_id} already exists in database")
                    skipped_count += 1
                    continue
                
                # Process withdrawal amount (handle USC conversion)
                amount_val = safe_str(row[amount_idx]).upper()
                amount = safe_float(amount_val)
                if 'USC' in amount_val:
                    amount /= 100
                
                withdrawal = CRMWithdrawals(
                    user_id=current_user.id,
                    request_id=safe_str(request_id),
                    review_time=parse_date_flexible(row[req_time_idx]),
                    trading_account=safe_str(row[trading_account_idx]),
                    withdrawal_amount=amount
                )
                
                db.session.add(withdrawal)
                logger.info(f"Row {i+1}: Added withdrawal record for request {request_id}")
                added_count += 1
                
            except Exception as e:
                logger.error(f"Row {i+1}: Error processing - {e}")
                skipped_count += 1
                continue
        
        db.session.commit()
        logger.info(f"Processing complete: {added_count} added, {skipped_count} skipped")
        return {'added_rows': added_count, 'total_rows': len(rows), 'skipped_rows': skipped_count}
        
    except Exception as e:
        logger.error(f"Fatal error processing CRM withdrawals: {e}")
        db.session.rollback()
        raise e

def process_crm_deposit(file_path, file_format='csv'):
    """Process CRM Deposit CSV/XLSX data"""
    logger.info(f"Processing CRM deposits from: {file_path}")
    
    try:
        data = read_file_with_encoding(file_path, file_format)
        
        if data.empty:
            raise ValueError("File is empty or invalid")
        
        headers = data.columns.tolist()
        rows = data.values.tolist()
        
        logger.info(f"File has {len(rows)} rows and {len(headers)} columns")
        logger.info(f"Headers: {headers}")
        
        # Find required columns
        req_idx = find_column_index(headers, ['Request Time', 'REQUEST_TIME'])
        acc_idx = find_column_index(headers, ['Trading Account', 'TRADING_ACCOUNT'])
        amt_idx = find_column_index(headers, ['Trading Amount', 'TRADING_AMOUNT'])
        id_idx = find_column_index(headers, ['Request ID', 'REQUEST_ID'])
        pay_method_idx = find_column_index(headers, ['Payment Method', 'PAYMENT_METHOD'])
        client_id_idx = find_column_index(headers, ['Client ID', 'CLIENT_ID'])
        name_idx = find_column_index(headers, ['Name'], 'Name')
        
        logger.info(f"Column indices - req: {req_idx}, acc: {acc_idx}, amt: {amt_idx}, id: {id_idx}")
        
        if None in [req_idx, acc_idx, amt_idx, id_idx]:
            missing = []
            if req_idx is None: missing.append("Request Time")
            if acc_idx is None: missing.append("Trading Account")
            if amt_idx is None: missing.append("Trading Amount")
            if id_idx is None: missing.append("Request ID")
            raise ValueError(f"Required columns not found: {', '.join(missing)}")
        
        added_count = 0
        skipped_count = 0
        
        for i, row in enumerate(rows):
            try:
                required_indices = [idx for idx in [req_idx, acc_idx, amt_idx, id_idx] if idx is not None]
                max_idx = max(required_indices)
                
                if len(row) <= max_idx:
                    logger.warning(f"Row {i+1}: Skipped - insufficient columns ({len(row)} <= {max_idx})")
                    skipped_count += 1
                    continue
                
                request_id = str(row[id_idx] or '').strip()
                if not request_id:
                    logger.warning(f"Row {i+1}: Skipped - empty request ID")
                    skipped_count += 1
                    continue
                
                logger.info(f"Row {i+1}: Processing request ID '{request_id}'")
                
                # Check if already exists
                existing = CRMDeposit.query.filter_by(request_id=request_id).first()
                if existing:
                    logger.info(f"Row {i+1}: Skipped - request ID {request_id} already exists in database")
                    skipped_count += 1
                    continue
                
                # Process trading amount (handle USC conversion)
                amount_val = safe_str(row[amt_idx]).upper()
                amount = safe_float(amount_val)
                if 'USC' in amount_val:
                    amount /= 100
                
                deposit = CRMDeposit(
                    user_id=current_user.id,
                    request_id=safe_str(request_id),
                    request_time=parse_date_flexible(row[req_idx]),
                    trading_account=safe_str(row[acc_idx]),
                    trading_amount=amount,
                    payment_method=safe_str(row[pay_method_idx]) if pay_method_idx is not None and pay_method_idx < len(row) else '',
                    client_id=safe_str(row[client_id_idx]) if client_id_idx is not None and client_id_idx < len(row) else '',
                    name=safe_str(row[name_idx]) if name_idx is not None and name_idx < len(row) else ''
                )
                
                db.session.add(deposit)
                logger.info(f"Row {i+1}: Added deposit record for request {request_id}")
                added_count += 1
                
            except Exception as e:
                logger.error(f"Row {i+1}: Error processing - {e}")
                skipped_count += 1
                continue
        
        db.session.commit()
        logger.info(f"Processing complete: {added_count} added, {skipped_count} skipped")
        return {'added_rows': added_count, 'total_rows': len(rows), 'skipped_rows': skipped_count}
        
    except Exception as e:
        logger.error(f"Fatal error processing CRM deposits: {e}")
        db.session.rollback()
        raise e

def process_account_list(file_path, file_format='csv'):
    """Process Account List CSV/XLSX data"""
    logger.info(f"Processing account list from: {file_path}")
    
    try:
        data = read_file_with_encoding(file_path, file_format)
        
        if data.empty:
            raise ValueError("File is empty or invalid")
        
        # Remove description line if present
        if len(data) > 0 and 'METATRADER' in str(data.iloc[0, 0]).upper():
            data = data.iloc[1:]
            logger.info("Removed MetaTrader description line")
        
        headers = data.columns.tolist()
        rows = data.values.tolist()
        
        logger.info(f"File has {len(rows)} rows and {len(headers)} columns")
        logger.info(f"Headers: {headers}")
        
        # Find required columns
        login_idx = find_column_index(headers, ['Login'], 'Login')
        name_idx = find_column_index(headers, ['Name'], 'Name')
        group_idx = find_column_index(headers, ['Group'], 'Group')
        
        logger.info(f"Column indices - login: {login_idx}, name: {name_idx}, group: {group_idx}")
        
        if None in [login_idx, name_idx, group_idx]:
            missing = []
            if login_idx is None: missing.append("Login")
            if name_idx is None: missing.append("Name")
            if group_idx is None: missing.append("Group")
            raise ValueError(f"Required columns not found: {', '.join(missing)}")
        
        # Clear existing account list for this user
        deleted_count = AccountList.query.filter_by(user_id=current_user.id).delete()
        logger.info(f"Deleted {deleted_count} existing account records for user")
        
        added_count = 0
        skipped_count = 0
        
        for i, row in enumerate(rows):
            try:
                max_idx = max(login_idx, name_idx, group_idx)
                if len(row) <= max_idx:
                    logger.warning(f"Row {i+1}: Skipped - insufficient columns ({len(row)} <= {max_idx})")
                    skipped_count += 1
                    continue
                
                login = safe_str(row[login_idx])
                name = safe_str(row[name_idx])
                group = safe_str(row[group_idx])
                
                if not login:
                    logger.warning(f"Row {i+1}: Skipped - empty login")
                    skipped_count += 1
                    continue
                
                logger.info(f"Row {i+1}: Processing login '{login}'")
                
                is_welcome = group == "WELCOME\\Welcome BBOOK"
                
                account = AccountList(
                    user_id=current_user.id,
                    login=login,
                    name=name,
                    group=group,
                    is_welcome_bonus=is_welcome
                )
                
                db.session.add(account)
                logger.info(f"Row {i+1}: Added account record for login {login}")
                added_count += 1
                
            except Exception as e:
                logger.error(f"Row {i+1}: Error processing - {e}")
                skipped_count += 1
                continue
        
        db.session.commit()
        logger.info(f"Processing complete: {added_count} added, {skipped_count} skipped")
        return {'added_rows': added_count, 'total_rows': len(rows), 'skipped_rows': skipped_count}
        
    except Exception as e:
        logger.error(f"Fatal error processing account list: {e}")
        db.session.rollback()
        raise e

# Utility function to check for existing records before processing
def check_existing_records(file_path, file_type, file_format='csv'):
    """
    Check how many records from the file already exist in the database
    This is useful for debugging why files show 0 added rows
    """
    logger.info(f"Checking existing records for {file_type} in {file_path}")
    
    try:
        data = read_file_with_encoding(file_path, file_format)
        
        if data.empty:
            return {"error": "File is empty"}
        
        headers = data.columns.tolist()
        rows = data.values.tolist()
        
        existing_count = 0
        new_count = 0
        
        if file_type.lower() == 'ib_rebate':
            tx_id_idx = find_column_index(headers, ['Transaction ID'], 'Transaction ID')
            if tx_id_idx is None:
                return {"error": "Transaction ID column not found"}
            
            for row in rows:
                if len(row) > tx_id_idx:
                    tx_id = str(row[tx_id_idx] or '').strip()
                    if tx_id:
                        existing = IBRebate.query.filter_by(transaction_id=tx_id).first()
                        if existing:
                            existing_count += 1
                        else:
                            new_count += 1
        
        elif file_type.lower() == 'payment_data':
            # Find transaction ID column for payment data
            tx_id_col = None
            for header in headers:
                if 'Transaction ID' in header:
                    tx_id_col = header
                    break
            
            if tx_id_col is None:
                return {"error": "Transaction ID column not found"}
            
            for i, row in enumerate(rows):
                row_dict = {}
                for j, header in enumerate(headers):
                    if j < len(row):
                        row_dict[header.strip()] = row[j]
                
                tx_id = str(row_dict.get(tx_id_col, '')).strip()
                if tx_id:
                    existing = PaymentData.query.filter_by(tx_id=tx_id).first()
                    if existing:
                        existing_count += 1
                    else:
                        new_count += 1
        
        elif file_type.lower() == 'crm_withdrawals':
            request_id_idx = find_column_index(headers, ['Request ID'], 'Request ID')
            if request_id_idx is None:
                return {"error": "Request ID column not found"}
            
            for row in rows:
                if len(row) > request_id_idx:
                    request_id = str(row[request_id_idx] or '').strip()
                    if request_id:
                        existing = CRMWithdrawals.query.filter_by(request_id=request_id).first()
                        if existing:
                            existing_count += 1
                        else:
                            new_count += 1
        
        elif file_type.lower() == 'crm_deposit':
            request_id_idx = find_column_index(headers, ['Request ID'], 'Request ID')
            if request_id_idx is None:
                return {"error": "Request ID column not found"}
            
            for row in rows:
                if len(row) > request_id_idx:
                    request_id = str(row[request_id_idx] or '').strip()
                    if request_id:
                        existing = CRMDeposit.query.filter_by(request_id=request_id).first()
                        if existing:
                            existing_count += 1
                        else:
                            new_count += 1
        
        return {
            "total_rows": len(rows),
            "existing_in_db": existing_count,
            "new_records": new_count,
            "headers": headers
        }
        
    except Exception as e:
        logger.error(f"Error checking existing records: {e}")
        return {"error": str(e)}

# Additional debugging function
def debug_file_processing(file_path, file_type, file_format='csv'):
    """
    Debug function to analyze file processing step by step
    Use this to understand why files might not be processed correctly
    """
    logger.info(f"=== DEBUGGING FILE PROCESSING ===")
    logger.info(f"File: {file_path}")
    logger.info(f"Type: {file_type}")
    logger.info(f"Format: {file_format}")
    
    try:
        # Step 1: Check file reading
        logger.info("Step 1: Reading file...")
        data = read_file_with_encoding(file_path, file_format)
        logger.info(f"✓ File read successfully: {len(data)} rows, {len(data.columns)} columns")
        
        # Step 2: Check headers
        logger.info("Step 2: Analyzing headers...")
        headers = data.columns.tolist()
        logger.info(f"Headers: {headers}")
        
        # Step 3: Check data sample
        logger.info("Step 3: Data sample...")
        if not data.empty:
            logger.info(f"First row: {data.iloc[0].to_dict()}")
        
        # Step 4: Check for specific file type requirements
        logger.info(f"Step 4: Checking {file_type} specific requirements...")
        
        if file_type.lower() == 'ib_rebate':
            tx_id_idx = find_column_index(headers, ['Transaction ID'], 'Transaction ID')
            rebate_idx = find_column_index(headers, ['Rebate'], 'Rebate')
            rebate_time_idx = find_column_index(headers, ['Rebate Time'], 'Rebate Time')
            
            logger.info(f"Required columns found:")
            logger.info(f"  - Transaction ID: {'✓' if tx_id_idx is not None else '✗'} (index: {tx_id_idx})")
            logger.info(f"  - Rebate: {'✓' if rebate_idx is not None else '✗'} (index: {rebate_idx})")
            logger.info(f"  - Rebate Time: {'✓' if rebate_time_idx is not None else '✗'} (index: {rebate_time_idx})")
            
            if tx_id_idx is not None:
                rows = data.values.tolist()
                valid_rows = 0
                for i, row in enumerate(rows[:5]):  # Check first 5 rows
                    tx_id = str(row[tx_id_idx] or '').strip() if len(row) > tx_id_idx else ''
                    if tx_id:
                        valid_rows += 1
                        existing = IBRebate.query.filter_by(transaction_id=tx_id).first()
                        logger.info(f"  Row {i+1}: tx_id='{tx_id}', exists_in_db={existing is not None}")
                    else:
                        logger.info(f"  Row {i+1}: Empty transaction ID")
                
                logger.info(f"Valid rows with transaction IDs: {valid_rows}/{min(5, len(rows))}")
        
        # Step 5: Check existing records
        logger.info("Step 5: Checking existing records...")
        existing_check = check_existing_records(file_path, file_type, file_format)
        logger.info(f"Existing records check: {existing_check}")
        
        logger.info("=== DEBUG COMPLETE ===")
        return existing_check
        
    except Exception as e:
        logger.error(f"Debug failed: {e}")
        return {"error": str(e)}

# Example usage functions for testing
def test_ib_rebate_processing(file_path):
    """Test IB rebate processing with detailed output"""
    logger.info("=== TESTING IB REBATE PROCESSING ===")
    
    # First debug the file
    debug_result = debug_file_processing(file_path, 'ib_rebate')
    logger.info(f"Debug result: {debug_result}")
    
    # Then process it
    if debug_result.get('new_records', 0) > 0:
        logger.info("Found new records, processing...")
        result = process_ib_rebate(file_path)
        logger.info(f"Processing result: {result}")
    else:
        logger.warning("No new records found - all records already exist in database")
    
    return debug_result

# Helper function to clear existing data for testing
def clear_user_data(data_type, user_id=None):
    """
    Clear existing data for testing purposes
    WARNING: This will delete data from the database!
    """
    if user_id is None:
        user_id = current_user.id
    
    logger.warning(f"CLEARING {data_type} data for user {user_id}")
    
    if data_type.lower() == 'ib_rebate':
        deleted = IBRebate.query.filter_by(user_id=user_id).delete()
    elif data_type.lower() == 'payment_data':
        deleted = PaymentData.query.filter_by(user_id=user_id).delete()
    elif data_type.lower() == 'crm_withdrawals':
        deleted = CRMWithdrawals.query.filter_by(user_id=user_id).delete()
    elif data_type.lower() == 'crm_deposit':
        deleted = CRMDeposit.query.filter_by(user_id=user_id).delete()
    elif data_type.lower() == 'account_list':
        deleted = AccountList.query.filter_by(user_id=user_id).delete()
    else:
        logger.error(f"Unknown data type: {data_type}")
        return 0
    
    db.session.commit()
    logger.warning(f"Deleted {deleted} records")
    return deleted
