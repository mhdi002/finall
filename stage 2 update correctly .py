import pandas as pd
import re
from datetime import datetime
from sqlalchemy import and_
from app import db
from app.models import (
    PaymentData,
    IBRebate,
    CRMWithdrawals,
    CRMDeposit,
    AccountList
)
from flask_login import current_user

# ──────── UTILITY FUNCTIONS ────────────────────────────────────────────────────

def detect_separator(line: str) -> str:
    tab_count = line.count('\t')
    comma_count = line.count(',')
    semicolon_count = line.count(';')
    if tab_count >= comma_count and tab_count >= semicolon_count:
        return '\t'
    if semicolon_count >= comma_count:
        return ';'
    return ','

def parse_date(val, formats=None):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    fmts = formats or [
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
        '%d.%m.%Y %H:%M:%S', '%d.%m.%Y',
        '%d/%m/%Y %H:%M:%S', '%d/%m/%Y',
        '%m/%d/%Y %H:%M:%S', '%m/%d/%Y'
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    return None

def read_csv(filepath: str) -> pd.DataFrame:
    """Try multiple encodings and detect separator, return DataFrame."""
    for enc in ['utf-8-sig','utf-8','latin1','cp1252','iso-8859-1']:
        try:
            with open(filepath, encoding=enc) as f:
                first = f.readline()
            sep = detect_separator(first)
            return pd.read_csv(filepath, sep=sep, encoding=enc)
        except Exception:
            continue
    raise ValueError(f"Cannot read {filepath}")

def filter_unique(rows, existing_keys: set, key_cols: list):
    """Mimic the Apps Script filterUnique: keep rows whose key (join of cols) is new."""
    unique = []
    for row in rows:
        key = '|'.join(str(row[i] if i < len(row) else '').strip().upper() for i in key_cols)
        if key and key not in existing_keys:
            existing_keys.add(key)
            unique.append(row)
    return unique

# ──────── STAGE 1: DATA PROCESSING ───────────────────────────────────────────────

# Column mapping for PaymentData
_COLUMN_MAP = {
    'confirmed':'Confirmed','txId':'Transaction ID',
    'transactionAddress':'Wallet address','status':'Status','type':'Type',
    'paymentGatewayName':'Payment gateway','finalAmount':'Transaction amount',
    'finalCurrency':'Transaction currency','transactionAmount':'Settlement amount',
    'transactionCurrencyDisplayName':'Settlement currency','processingFee':'Processing fee',
    'price':'Price','comment':'Comment','paymentId':'Payment ID','created':'Booked',
    'tradingAccount':'Trading account','correctCoinSent':'correctCoinSent',
    'balanceAfterTransaction':'Balance after','tierFee':'Tier fee'
}
_SHEET_CATEGORIES = {
    'DEPOSIT': ('M2p Deposit','Settlement Deposit'),
    'WITHDRAW': ('M2p Withdraw','Settlement Withdraw')
}

def process_payment(filepath: str) -> int:
    df = read_csv(filepath)
    added = 0
    for _, row in df.iterrows():
        data = {h.strip(): row[i] for i,h in enumerate(df.columns)}
        tx = str(data.get(_COLUMN_MAP['txId'],'')).strip()
        status = str(data.get(_COLUMN_MAP['status'], '')).upper()
        pg = str(data.get(_COLUMN_MAP['paymentGatewayName'], '')).upper()
        ty = str(data.get(_COLUMN_MAP['type'], '')).upper()
        if not tx or pg == 'BALANCE' or status != 'DONE':
            continue
        if PaymentData.query.filter_by(tx_id=tx, user_id=current_user.id).first():
            continue

        # build record
        rec = PaymentData(
            user_id=current_user.id,
            confirmed=parse_date(data.get(_COLUMN_MAP['confirmed'])),
            tx_id=tx,
            wallet_address=data.get(_COLUMN_MAP['transactionAddress']),
            status=status,
            type=ty,
            payment_gateway=data.get(_COLUMN_MAP['paymentGatewayName']),
            final_amount=float(data.get(_COLUMN_MAP['finalAmount']) or 0),
            final_currency=data.get(_COLUMN_MAP['finalCurrency']),
            settlement_amount=float(data.get(_COLUMN_MAP['transactionAmount']) or 0),
            settlement_currency=data.get(_COLUMN_MAP['transactionCurrencyDisplayName']),
            processing_fee=float(data.get(_COLUMN_MAP['processingFee']) or 0),
            price=float(data.get(_COLUMN_MAP['price']) or 1),
            comment=data.get(_COLUMN_MAP['comment']),
            payment_id=data.get(_COLUMN_MAP['paymentId']),
            created=parse_date(data.get(_COLUMN_MAP['created'])),
            trading_account=data.get(_COLUMN_MAP['tradingAccount']),
            correct_coin_sent=True,
            balance_after=float(data.get(_COLUMN_MAP['balanceAfterTransaction']) or 0),
            tier_fee=float(data.get(_COLUMN_MAP['tierFee']) or 0),
            sheet_category=(
                _SHEET_CATEGORIES['DEPOSIT'][1] if ty=='DEPOSIT' and 'SETTLEMENT' in pg
                else _SHEET_CATEGORIES['DEPOSIT'][0] if ty=='DEPOSIT'
                else _SHEET_CATEGORIES['WITHDRAW'][1] if ty=='WITHDRAW' and 'SETTLEMENT' in pg
                else _SHEET_CATEGORIES['WITHDRAW'][0]
            )
        )
        db.session.add(rec)
        added += 1

    db.session.commit()
    return added

def process_ib_rebate(filepath: str) -> int:
    df = read_csv(filepath)
    added = 0
    for _, row in df.iterrows():
        tx = str(row.get('Transaction ID','')).strip()
        if not tx or IBRebate.query.filter_by(transaction_id=tx, user_id=current_user.id).first():
            continue
        rec = IBRebate(
            user_id=current_user.id,
            transaction_id=tx,
            rebate=float(row.get('Rebate',0) or 0),
            rebate_time=parse_date(row.get('Rebate Time',''))
        )
        db.session.add(rec)
        added += 1
    db.session.commit()
    return added

def process_crm_withdrawals(filepath: str) -> int:
    df = read_csv(filepath)
    added = 0
    for _, row in df.iterrows():
        req = str(row.get('Request ID','')).strip()
        if not req or CRMWithdrawals.query.filter_by(request_id=req, user_id=current_user.id).first():
            continue
        amt_raw = str(row.get('Withdrawal Amount','')).upper()
        if 'USC' in amt_raw:
            amt = float(re.sub(r'[^0-9.-]','', amt_raw)) / 100
        else:
            amt = float(re.sub(r'[^0-9.-]','', amt_raw) or 0)
        rec = CRMWithdrawals(
            user_id=current_user.id,
            request_id=req,
            review_time=parse_date(row.get('Review Time','')),
            trading_account=str(row.get('Trading Account','')).strip(),
            withdrawal_amount=amt
        )
        db.session.add(rec)
        added += 1
    db.session.commit()
    return added

def process_crm_deposit(filepath: str) -> int:
    df = read_csv(filepath)
    added = 0
    for _, row in df.iterrows():
        req = str(row.get('Request ID','')).strip()
        if not req or CRMDeposit.query.filter_by(request_id=req, user_id=current_user.id).first():
            continue
        amt_raw = str(row.get('Trading Amount',''))
        if 'USC' in amt_raw.upper():
            parts = amt_raw.split()
            num = re.sub(r'[^0-9.-]','', parts[1] if len(parts)>1 else '0')
            amt = float(num) / 100
        else:
            amt = float(re.sub(r'[^0-9.-]','', amt_raw) or 0)
        rec = CRMDeposit(
            user_id=current_user.id,
            request_id=req,
            request_time=parse_date(row.get('Request Time','')),
            trading_account=str(row.get('Trading Account','')).strip(),
            trading_amount=amt,
            payment_method=row.get('Payment Method',''),
            client_id=row.get('Client ID',''),
            name=row.get('Name','')
        )
        db.session.add(rec)
        added += 1
    db.session.commit()
    return added

def process_account_list(filepath: str) -> int:
    df = read_csv(filepath)
    # remove first METATRADER line if present
    if 'METATRADER' in df.iloc[0,0].upper():
        df = df.iloc[1:]
    # clear previous
    AccountList.query.filter_by(user_id=current_user.id).delete()
    added = 0
    for _, row in df.iterrows():
        login = str(row.get('Login','')).strip()
        if not login:
            continue
        grp = str(row.get('Group','')).strip()
        rec = AccountList(
            user_id=current_user.id,
            login=login,
            name=row.get('Name',''),
            group=grp,
            is_welcome_bonus=(grp == r'WELCOME\Welcome BBOOK')
        )
        db.session.add(rec)
        added += 1
    db.session.commit()
    return added

# ──────── STAGE 2: REPORT GENERATION ────────────────────────────────────────────

def filter_by_date_range(query, start_date, end_date, column):
    if start_date and end_date:
        return query.filter(and_(column >= start_date, column <= end_date))
    return query

def sum_column(records, attr):
    return sum(getattr(r, attr, 0) or 0 for r in records)

def generate_final_report(start_date=None, end_date=None):
    pay_q = filter_by_date_range(
        PaymentData.query.filter_by(user_id=current_user.id),
        start_date, end_date, PaymentData.created
    )
    rebate_q = filter_by_date_range(
        IBRebate.query.filter_by(user_id=current_user.id),
        start_date, end_date, IBRebate.rebate_time
    )
    crm_w_q = filter_by_date_range(
        CRMWithdrawals.query.filter_by(user_id=current_user.id),
        start_date, end_date, CRMWithdrawals.review_time
    )
    crm_d_q = filter_by_date_range(
        CRMDeposit.query.filter_by(user_id=current_user.id),
        start_date, end_date, CRMDeposit.request_time
    )

    calc = {
        'Total Rebate':      sum_column(rebate_q.all(),     'rebate'),
        'M2p Deposit':       sum(r.final_amount for r in pay_q.filter_by(sheet_category='M2p Deposit').all()),
        'Settlement Deposit':sum(r.final_amount for r in pay_q.filter_by(sheet_category='Settlement Deposit').all()),
        'M2p Withdrawal':    sum(r.final_amount for r in pay_q.filter_by(sheet_category='M2p Withdraw').all()),
        'Settlement Withdrawal':sum(r.final_amount for r in pay_q.filter_by(sheet_category='Settlement Withdraw').all()),
        'CRM Deposit Total': sum_column(crm_d_q.all(),       'trading_amount'),
        'CRM Withdraw Total':sum_column(crm_w_q.all(),      'withdrawal_amount'),
        'Tier Fee Deposit':  sum(r.tier_fee for r in pay_q.filter(PaymentData.sheet_category.ilike('%Deposit'))),
        'Tier Fee Withdraw': sum(r.tier_fee for r in pay_q.filter(PaymentData.sheet_category.ilike('%Withdraw')))
    }

    # Welcome bonus
    welcome_ids = [a.login for a in AccountList.query.filter_by(
        user_id=current_user.id, is_welcome_bonus=True
    ).all()]
    wb = 0
    for w in crm_w_q.all():
        m = re.search(r'\d+', w.trading_account or '')
        if m and m.group() in welcome_ids:
            wb += w.withdrawal_amount or 0
    calc['Welcome Bonus Withdrawals'] = wb

    # TopChange
    tc = sum(d.trading_amount for d in crm_d_q.all()
             if (d.payment_method or '').upper() == 'TOPCHANGE')
    calc['CRM TopChange Total'] = tc

    # build report rows
    report = []
    if start_date and end_date:
        report.append(['Date Range', f"From {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}"])
        report.append(['',''])
    for key, val in calc.items():
        report.append([key, f"{val:.2f}"])
    return report

def generate_filtered_final_report(start_date, end_date):
    # same as above, but using only filtered query sets
    pay_q = filter_by_date_range(
        PaymentData.query.filter_by(user_id=current_user.id),
        start_date, end_date, PaymentData.created
    )
    rebate_q = filter_by_date_range(
        IBRebate.query.filter_by(user_id=current_user.id),
        start_date, end_date, IBRebate.rebate_time
    )
    crm_w_q = filter_by_date_range(
        CRMWithdrawals.query.filter_by(user_id=current_user.id),
        start_date, end_date, CRMWithdrawals.review_time
    )
    crm_d_q = filter_by_date_range(
        CRMDeposit.query.filter_by(user_id=current_user.id),
        start_date, end_date, CRMDeposit.request_time
    )

    report = []
    report.append(['Filtered Date Range', f"From {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}"])
    report.append(['',''])
    report.append(['Total Rebate', f"{sum_column(rebate_q.all(), 'rebate'):.2f}"])
    report.append(['M2p Deposit', f"{sum(r.final_amount for r in pay_q.filter_by(sheet_category='M2p Deposit').all()):.2f}"])
    report.append(['Settlement Deposit', f"{sum(r.final_amount for r in pay_q.filter_by(sheet_category='Settlement Deposit').all()):.2f}"])
    report.append(['M2p Withdrawal', f"{sum(r.final_amount for r in pay_q.filter_by(sheet_category='M2p Withdraw').all()):.2f}"])
    report.append(['Settlement Withdrawal', f"{sum(r.final_amount for r in pay_q.filter_by(sheet_category='Settlement Withdraw').all()):.2f}"])
    report.append(['CRM Deposit Total', f"{sum_column(crm_d_q.all(), 'trading_amount'):.2f}"])
    report.append(['TopChange Deposit Total',
                   f"{sum(d.trading_amount for d in crm_d_q.all() if (d.payment_method or '').upper()=='TOPCHANGE'):.2f}"])
    tf_dep = sum(r.tier_fee for r in pay_q.filter(PaymentData.sheet_category.ilike('%Deposit')))
    tf_wdr = sum(r.tier_fee for r in pay_q.filter(PaymentData.sheet_category.ilike('%Withdraw')))
    report.append(['Tier Fee Deposit', f"{tf_dep:.2f}"])
    report.append(['Tier Fee Withdraw', f"{tf_wdr:.2f}"])
    # Welcome bonus
    welcome_ids = [a.login for a in AccountList.query.filter_by(user_id=current_user.id, is_welcome_bonus=True).all()]
    wb = 0
    for w in crm_w_q.all():
        m = re.search(r'\d+', w.trading_account or '')
        if m and m.group() in welcome_ids:
            wb += w.withdrawal_amount or 0
    report.append(['Welcome Bonus Withdrawals', f"{wb:.2f}"])
    report.append(['CRM Withdraw Total', f"{sum_column(crm_w_q.all(), 'withdrawal_amount'):.2f}"])
    return report

def compare_deposits(start_date=None, end_date=None):
    """Implement deposit vs CRM deposit discrepancy logic here, if desired."""
    pass
