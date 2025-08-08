from app import db, login_manager
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import uuid

class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    users = db.relationship('User', backref='role', lazy='dynamic')

    def __repr__(self):
        return f'<Role {self.name}>'

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    email = db.Column(db.String(120), index=True, unique=True)
    password_hash = db.Column(db.String(128))
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    logs = db.relationship('Log', backref='user', lazy='dynamic')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def has_role(self, role_name):
        return self.role is not None and self.role.name == role_name

    def __repr__(self):
        return f'<User {self.username}>'

class Log(db.Model):
    __tablename__ = 'logs'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    action = db.Column(db.String(128))
    details = db.Column(db.String(256), nullable=True)
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)

    def __repr__(self):
        return f'<Log {self.user.username} - {self.action}>'

# Stage 2: New Models for Enhanced Financial Data Processing

class PaymentData(db.Model):
    __tablename__ = 'payment_data'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    confirmed = db.Column(db.String(20))
    tx_id = db.Column(db.String(100), unique=True, index=True)
    wallet_address = db.Column(db.String(200))
    status = db.Column(db.String(20))
    type = db.Column(db.String(20))  # DEPOSIT/WITHDRAW
    payment_gateway = db.Column(db.String(100))
    final_amount = db.Column(db.Float)
    final_currency = db.Column(db.String(10))
    settlement_amount = db.Column(db.Float)
    settlement_currency = db.Column(db.String(10))
    processing_fee = db.Column(db.Float)
    price = db.Column(db.Float, default=1.0)
    comment = db.Column(db.Text)
    payment_id = db.Column(db.String(100))
    created = db.Column(db.DateTime)
    trading_account = db.Column(db.String(50))
    correct_coin_sent = db.Column(db.Boolean, default=True)
    balance_after = db.Column(db.Float)
    tier_fee = db.Column(db.Float)
    sheet_category = db.Column(db.String(50))  # M2p Deposit, Settlement Deposit, etc.
    upload_timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class IBRebate(db.Model):
    __tablename__ = 'ib_rebate'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    transaction_id = db.Column(db.String(100), unique=True, index=True)
    rebate = db.Column(db.Float)
    rebate_time = db.Column(db.DateTime)
    upload_timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class CRMWithdrawals(db.Model):
    __tablename__ = 'crm_withdrawals'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    request_id = db.Column(db.String(100), unique=True, index=True)
    review_time = db.Column(db.DateTime)
    trading_account = db.Column(db.String(50))
    withdrawal_amount = db.Column(db.Float)
    upload_timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class CRMDeposit(db.Model):
    __tablename__ = 'crm_deposit'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    request_id = db.Column(db.String(100), unique=True, index=True)
    request_time = db.Column(db.DateTime)
    trading_account = db.Column(db.String(50))
    trading_amount = db.Column(db.Float)
    payment_method = db.Column(db.String(50))
    client_id = db.Column(db.String(50))
    name = db.Column(db.String(200))
    upload_timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class AccountList(db.Model):
    __tablename__ = 'account_list'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    login = db.Column(db.String(50), unique=True, index=True)
    name = db.Column(db.String(200))
    group = db.Column(db.String(100))
    is_welcome_bonus = db.Column(db.Boolean, default=False)
    upload_timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class UploadedFiles(db.Model):
    __tablename__ = 'uploaded_files'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    file_type = db.Column(db.String(50))  # deals, excluded, vip, payment, ib_rebate, crm_withdrawals, crm_deposit, account_list
    filename = db.Column(db.String(200))
    file_path = db.Column(db.String(500))
    upload_timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    processed = db.Column(db.Boolean, default=False)

@login_manager.user_loader
def load_user(id):
    return User.query.get(int(id))