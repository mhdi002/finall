from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed, FileRequired
from wtforms import StringField, PasswordField, BooleanField, SubmitField, SelectField, DateTimeField
from wtforms.validators import DataRequired, ValidationError, Email, EqualTo, Optional
from app.models import User
import re

class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign In')

class RegistrationForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired()])
    password2 = PasswordField(
        'Repeat Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Register')

    def validate_username(self, username):
        user = User.query.filter_by(username=username.data).first()
        if user is not None:
            raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
        if user is not None:
            raise ValidationError('Please use a different email address.')

    def validate_password(self, password):
        p = password.data
        if len(p) < 8:
            raise ValidationError('Password must be at least 8 characters long.')
        if not re.search(r'[A-Z]', p):
            raise ValidationError('Password must contain at least one uppercase letter.')
        if not re.search(r'[a-z]', p):
            raise ValidationError('Password must contain at least one lowercase letter.')
        if not re.search(r'[\W_]', p): # \W is any non-alphanumeric character
            raise ValidationError('Password must contain at least one symbol.')

# Stage 2: Enhanced File Upload Forms

class DynamicUploadForm(FlaskForm):
    # Original deal processing files
    deals_csv = FileField('Deals CSV/XLSX File', 
                         validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                         render_kw={"accept": ".csv,.xlsx"})
    excluded_csv = FileField('Excluded Accounts CSV/XLSX File', 
                            validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                            render_kw={"accept": ".csv,.xlsx"})
    vip_csv = FileField('VIP Clients CSV/XLSX File', 
                       validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                       render_kw={"accept": ".csv,.xlsx"})
    
    # Stage 2: New file types
    payment_data = FileField('Payment Data CSV/XLSX File', 
                            validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                            render_kw={"accept": ".csv,.xlsx"})
    ib_rebate = FileField('IB Rebate CSV/XLSX File', 
                         validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                         render_kw={"accept": ".csv,.xlsx"})
    crm_withdrawals = FileField('CRM Withdrawals CSV/XLSX File', 
                               validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                               render_kw={"accept": ".csv,.xlsx"})
    crm_deposit = FileField('CRM Deposit CSV/XLSX File', 
                           validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                           render_kw={"accept": ".csv,.xlsx"})
    account_list = FileField('Account List CSV/XLSX File', 
                            validators=[FileAllowed(['csv', 'xlsx'], 'Only CSV and XLSX files allowed!')],
                            render_kw={"accept": ".csv,.xlsx"})
    
    submit = SubmitField('Upload Selected Files')

class DateRangeForm(FlaskForm):
    start_date = DateTimeField('Start Date', 
                              validators=[Optional()],
                              format='%Y-%m-%d %H:%M:%S',
                              render_kw={"placeholder": "YYYY-MM-DD HH:MM:SS (optional)"})
    end_date = DateTimeField('End Date', 
                            validators=[Optional()],
                            format='%Y-%m-%d %H:%M:%S',
                            render_kw={"placeholder": "YYYY-MM-DD HH:MM:SS (optional)"})
    
    submit = SubmitField('Generate Report')

    def validate(self, **kwargs):
        if not super().validate(**kwargs):
            return False
        
        if self.start_date.data and self.end_date.data:
            if self.start_date.data >= self.end_date.data:
                self.end_date.errors.append('End date must be after start date.')
                return False
        
        return True