import os
import pandas as pd
from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app, session, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename

from app import db
from app.models import User, Role, Log, UploadedFiles
from app.forms import LoginForm, RegistrationForm, DynamicUploadForm, DateRangeForm
from app.processing import run_report_processing
from app.charts import create_charts, create_stage2_charts
from app.logger import record_log

# Stage 2 imports
from app.stage2_processing import (
    process_payment_data, process_ib_rebate, process_crm_withdrawals, 
    process_crm_deposit, process_account_list
)
from app.stage2_reports_enhanced import generate_final_report, compare_crm_and_client_deposits, get_summary_data_for_charts, check_data_sufficiency_for_charts

bp = Blueprint('main', __name__)

@bp.route('/')
@bp.route('/index')
def index():
    return render_template('index.html', title='Home')

@bp.route('/dashboard')
@login_required
def dashboard():
    # Get upload status for current user
    uploaded_files = UploadedFiles.query.filter_by(user_id=current_user.id).all()
    file_status = {}
    
    for file_record in uploaded_files:
        file_status[file_record.file_type] = {
            'filename': file_record.filename,
            'uploaded': True,
            'processed': file_record.processed,
            'timestamp': file_record.upload_timestamp
        }
    
    form = DateRangeForm()
    return render_template('dashboard.html', title='Dashboard', file_status=file_status, form=form)

@bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password', 'danger')
            return redirect(url_for('main.login'))
        login_user(user, remember=form.remember_me.data, duration=None)
        session.permanent = True
        record_log('user_login')
        return redirect(url_for('main.dashboard'))
    return render_template('login.html', title='Sign In', form=form)

@bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    form = RegistrationForm()
    if form.validate_on_submit():
        viewer_role = Role.query.filter_by(name='Viewer').first()
        if not viewer_role:
            flash('System error: User roles not configured.', 'danger')
            return redirect(url_for('main.register'))

        user = User(username=form.username.data, email=form.email.data, role=viewer_role)
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        flash('Congratulations, you are now a registered user!', 'success')
        return redirect(url_for('main.login'))
    return render_template('register.html', title='Register', form=form)

@bp.route('/logout')
@login_required
def logout():
    record_log('user_logout')
    logout_user()
    return redirect(url_for('main.index'))

# Enhanced Upload Route - Supporting Dynamic File Upload
@bp.route('/upload', methods=['GET', 'POST'])
@login_required
def upload_file():
    form = DynamicUploadForm()
    
    if form.validate_on_submit():
        upload_folder = current_app.config['UPLOAD_FOLDER']
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
        
        uploaded_any = False
        processing_results = {}
        
        # Define file type mappings
        file_mappings = {
            'deals_csv': ('deals', 'Deal Processing'),
            'excluded_csv': ('excluded', 'Excluded Accounts'),
            'vip_csv': ('vip', 'VIP Clients'),
            'payment_data': ('payment', 'Payment Data'),
            'ib_rebate': ('ib_rebate', 'IB Rebate'),
            'crm_withdrawals': ('crm_withdrawals', 'CRM Withdrawals'),
            'crm_deposit': ('crm_deposit', 'CRM Deposit'),
            'account_list': ('account_list', 'Account List')
        }
        
        # Process each uploaded file
        for field_name, (file_type, display_name) in file_mappings.items():
            file_field = getattr(form, field_name)
            if file_field.data and file_field.data.filename:
                uploaded_any = True
                
                filename = secure_filename(file_field.data.filename)
                file_extension = filename.rsplit('.', 1)[1].lower()
                safe_filename = f"{file_type}_{current_user.id}_{int(pd.Timestamp.now().timestamp())}.{file_extension}"
                file_path = os.path.join(upload_folder, safe_filename)
                
                file_field.data.save(file_path)
                
                # Record upload in database
                uploaded_file = UploadedFiles(
                    user_id=current_user.id,
                    file_type=file_type,
                    filename=filename,
                    file_path=file_path
                )
                db.session.add(uploaded_file)
                
                # Process Stage 2 files immediately
                try:
                    if file_type == 'payment':
                        result = process_payment_data(file_path, file_extension)
                        processing_results[display_name] = f"Added {result['added_rows']} rows"
                        uploaded_file.processed = True
                        
                    elif file_type == 'ib_rebate':
                        result = process_ib_rebate(file_path, file_extension)
                        processing_results[display_name] = f"Added {result['added_rows']} rows"
                        uploaded_file.processed = True
                        
                    elif file_type == 'crm_withdrawals':
                        result = process_crm_withdrawals(file_path, file_extension)
                        processing_results[display_name] = f"Added {result['added_rows']} rows"
                        uploaded_file.processed = True
                        
                    elif file_type == 'crm_deposit':
                        result = process_crm_deposit(file_path, file_extension)
                        processing_results[display_name] = f"Added {result['added_rows']} rows"
                        uploaded_file.processed = True
                        
                    elif file_type == 'account_list':
                        result = process_account_list(file_path, file_extension)
                        processing_results[display_name] = f"Added {result['added_rows']} rows"
                        uploaded_file.processed = True
                        
                    else:
                        # Original files (deals, excluded, vip) - just mark as uploaded
                        processing_results[display_name] = "Uploaded successfully"
                        
                except Exception as e:
                    flash(f'Error processing {display_name}: {str(e)}', 'warning')
                    processing_results[display_name] = f"Upload successful, processing failed: {str(e)}"
        
        if uploaded_any:
            db.session.commit()
            record_log('files_uploaded', f"Uploaded: {', '.join(processing_results.keys())}")
            
            # Create summary message
            summary_msg = "Files processed successfully:\n"
            for file_type, result in processing_results.items():
                summary_msg += f"• {file_type}: {result}\n"
            
            flash(summary_msg, 'success')
            session['files_uploaded'] = True
        else:
            flash('No files were selected for upload.', 'warning')
        
        return redirect(url_for('main.dashboard'))
    
    return render_template('upload.html', title='Upload Files', form=form)

# Original Report Generation (keeping existing functionality)
@bp.route('/report/generate')
@login_required
def generate_report():
    # Check if original files are uploaded
    original_files = ['deals', 'excluded', 'vip']
    missing_files = []
    
    for file_type in original_files:
        file_record = UploadedFiles.query.filter_by(
            user_id=current_user.id, 
            file_type=file_type
        ).first()
        if not file_record:
            missing_files.append(file_type.replace('_', ' ').title())
    
    if missing_files:
        flash(f'Please upload the following files first: {", ".join(missing_files)}', 'warning')
        return redirect(url_for('main.upload_file'))

    # Get file paths
    deals_file = UploadedFiles.query.filter_by(user_id=current_user.id, file_type='deals').first()
    excluded_file = UploadedFiles.query.filter_by(user_id=current_user.id, file_type='excluded').first()
    vip_file = UploadedFiles.query.filter_by(user_id=current_user.id, file_type='vip').first()

    try:
        # Load data based on file extension
        deals_ext = deals_file.filename.rsplit('.', 1)[1].lower()
        if deals_ext == 'xlsx':
            deals_df = pd.read_excel(deals_file.file_path)
        else:
            deals_df = pd.read_csv(deals_file.file_path)
        
        excluded_ext = excluded_file.filename.rsplit('.', 1)[1].lower()
        if excluded_ext == 'xlsx':
            excluded_df = pd.read_excel(excluded_file.file_path, header=None)
        else:
            excluded_df = pd.read_csv(excluded_file.file_path, header=None)
        
        vip_ext = vip_file.filename.rsplit('.', 1)[1].lower()
        if vip_ext == 'xlsx':
            vip_df = pd.read_excel(vip_file.file_path, header=None)
        else:
            vip_df = pd.read_csv(vip_file.file_path, header=None)

        results = run_report_processing(deals_df, excluded_df, vip_df)

        # Convert result tables to HTML
        report_tables = {
            key: df.to_html(classes='table table-striped table-hover', index=False)
            for key, df in results.items() if isinstance(df, pd.DataFrame)
        }

        # Generate charts
        report_charts = create_charts(results)

        record_log('report_generated')

        return render_template('results.html', 
                             title='Deal Processing Results', 
                             tables=report_tables, 
                             charts=report_charts,
                             report_type='original')

    except Exception as e:
        flash(f'An error occurred during report generation: {e}', 'danger')
        return redirect(url_for('main.dashboard'))

# Stage 2: New Report Generation Routes
@bp.route('/report/stage2', methods=['POST'])
@login_required
def generate_stage2_report():
    form = DateRangeForm()
    
    if form.validate_on_submit():
        try:
            start_date = form.start_date.data
            end_date = form.end_date.data
            
            # Generate Stage 2 financial report
            report = generate_final_report(start_date, end_date)
            chart_data = get_summary_data_for_charts(start_date, end_date)
            
            # Generate Stage 2 specific charts
            stage2_charts = create_stage2_charts(chart_data)
            
            return render_template('stage2_results_enhanced.html',
                                    title='Financial Summary Report',
                                    report=report,
                                    chart_data=chart_data,
                                    charts=stage2_charts,
                                    start_date=start_date,
                                    end_date=end_date)

        except Exception as e:
            flash(f'Error generating report: {str(e)}', 'danger')
    
    # If form fails validation or an exception occurs, redirect to dashboard
    return redirect(url_for('main.dashboard'))

@bp.route('/api/upload_status')
@login_required
def upload_status():
    """API endpoint to get current upload status"""
    files = UploadedFiles.query.filter_by(user_id=current_user.id).all()
    
    status = {}
    for file_record in files:
        status[file_record.file_type] = {
            'filename': file_record.filename,
            'uploaded': True,
            'processed': file_record.processed,
            'timestamp': file_record.upload_timestamp.isoformat() if file_record.upload_timestamp else None
        }
    
    return jsonify(status)

@bp.route('/admin')
@login_required
def admin():
    if not current_user.has_role('Owner'):
        flash('You do not have permission to access the admin panel.', 'danger')
        return redirect(url_for('main.dashboard'))

    logs = Log.query.order_by(Log.timestamp.desc()).all()
    return render_template('admin.html', title='Admin Panel', logs=logs)