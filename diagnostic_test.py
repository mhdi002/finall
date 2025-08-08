#!/usr/bin/env python3
"""
Diagnostic test to understand Flask form handling and authentication issues
"""

import requests
import re
from bs4 import BeautifulSoup

BASE_URL = "http://127.0.0.1:5000"

def diagnose_registration():
    """Diagnose registration form issues"""
    print("🔍 Diagnosing Registration Form...")
    
    session = requests.Session()
    
    # Get registration page
    response = session.get(f"{BASE_URL}/register")
    print(f"Registration page status: {response.status_code}")
    
    if response.status_code == 200:
        # Parse the HTML to understand the form structure
        soup = BeautifulSoup(response.text, 'html.parser')
        form = soup.find('form')
        
        if form:
            print("Form found. Form fields:")
            inputs = form.find_all(['input', 'select', 'textarea'])
            for inp in inputs:
                name = inp.get('name', 'unnamed')
                input_type = inp.get('type', inp.name)
                print(f"  - {name}: {input_type}")
                
            # Try to extract CSRF token
            csrf_input = form.find('input', {'name': 'csrf_token'})
            if csrf_input:
                csrf_token = csrf_input.get('value')
                print(f"CSRF token found: {csrf_token[:20]}...")
                
                # Try registration with proper form data
                reg_data = {
                    'username': 'sarah_analyst',
                    'email': 'sarah.analyst@financecorp.com',
                    'password': 'SecurePass123!',
                    'password2': 'SecurePass123!',
                    'csrf_token': csrf_token,
                    'submit': 'Register'
                }
                
                print("Attempting registration with form data...")
                reg_response = session.post(f"{BASE_URL}/register", data=reg_data)
                print(f"Registration response status: {reg_response.status_code}")
                
                if reg_response.status_code == 302:
                    print("✅ Registration successful (redirected)")
                    return True
                else:
                    print("Registration response content (first 500 chars):")
                    print(reg_response.text[:500])
                    
                    # Check for validation errors
                    soup_resp = BeautifulSoup(reg_response.text, 'html.parser')
                    errors = soup_resp.find_all(class_=re.compile(r'error|alert|danger'))
                    if errors:
                        print("Validation errors found:")
                        for error in errors:
                            print(f"  - {error.get_text().strip()}")
            else:
                print("❌ No CSRF token found in form")
        else:
            print("❌ No form found on registration page")
    
    return False

def diagnose_login():
    """Diagnose login form issues"""
    print("\n🔍 Diagnosing Login Form...")
    
    session = requests.Session()
    
    # Get login page
    response = session.get(f"{BASE_URL}/login")
    print(f"Login page status: {response.status_code}")
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        form = soup.find('form')
        
        if form:
            print("Login form found. Form fields:")
            inputs = form.find_all(['input', 'select', 'textarea'])
            for inp in inputs:
                name = inp.get('name', 'unnamed')
                input_type = inp.get('type', inp.name)
                print(f"  - {name}: {input_type}")
                
            # Try to extract CSRF token
            csrf_input = form.find('input', {'name': 'csrf_token'})
            if csrf_input:
                csrf_token = csrf_input.get('value')
                print(f"CSRF token found: {csrf_token[:20]}...")
                
                # Try login with a user that might exist
                login_data = {
                    'username': 'sarah_analyst',
                    'password': 'SecurePass123!',
                    'csrf_token': csrf_token,
                    'submit': 'Sign In'
                }
                
                print("Attempting login...")
                login_response = session.post(f"{BASE_URL}/login", data=login_data)
                print(f"Login response status: {login_response.status_code}")
                
                if login_response.status_code == 302:
                    print("✅ Login successful (redirected)")
                    return True, session
                else:
                    print("Login response content (first 500 chars):")
                    print(login_response.text[:500])
                    
                    # Check for validation errors
                    soup_resp = BeautifulSoup(login_response.text, 'html.parser')
                    errors = soup_resp.find_all(class_=re.compile(r'error|alert|danger'))
                    if errors:
                        print("Login errors found:")
                        for error in errors:
                            print(f"  - {error.get_text().strip()}")
            else:
                print("❌ No CSRF token found in login form")
        else:
            print("❌ No form found on login page")
    
    return False, None

def check_database_users():
    """Check if there are any users in the database"""
    print("\n🔍 Checking Database Users...")
    
    try:
        # Try to access the Flask shell context to check users
        import sys
        sys.path.append('/app')
        
        from app import create_app, db
        from app.models import User, Role
        
        app = create_app()
        with app.app_context():
            users = User.query.all()
            roles = Role.query.all()
            
            print(f"Users in database: {len(users)}")
            for user in users:
                print(f"  - {user.username} ({user.email}) - Role: {user.role.name if user.role else 'None'}")
                
            print(f"Roles in database: {len(roles)}")
            for role in roles:
                print(f"  - {role.name}")
                
            return len(users) > 0
            
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        return False

def test_file_upload_direct():
    """Test file upload without authentication to see form structure"""
    print("\n🔍 Testing File Upload Form Structure...")
    
    session = requests.Session()
    response = session.get(f"{BASE_URL}/upload")
    print(f"Upload page status: {response.status_code}")
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        form = soup.find('form')
        
        if form:
            print("Upload form found. Form fields:")
            inputs = form.find_all(['input', 'select', 'textarea'])
            for inp in inputs:
                name = inp.get('name', 'unnamed')
                input_type = inp.get('type', inp.name)
                print(f"  - {name}: {input_type}")
        else:
            print("❌ No form found on upload page")
    elif response.status_code == 302:
        print("Upload page redirected (authentication required)")
    
if __name__ == "__main__":
    print("🚀 Flask Backend Diagnostic Tests")
    print("=" * 50)
    
    # Check database first
    has_users = check_database_users()
    
    # Test registration
    reg_success = diagnose_registration()
    
    # Test login
    login_success, session = diagnose_login()
    
    # Test file upload form
    test_file_upload_direct()
    
    print("\n📊 DIAGNOSTIC SUMMARY:")
    print(f"Database has users: {'✅' if has_users else '❌'}")
    print(f"Registration working: {'✅' if reg_success else '❌'}")
    print(f"Login working: {'✅' if login_success else '❌'}")