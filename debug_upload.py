#!/usr/bin/env python3
"""
Debug file upload form structure
"""

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://127.0.0.1:5001"

def debug_upload_form():
    """Debug the upload form structure"""
    session = requests.Session()
    
    # First login
    print("🔍 Logging in...")
    login_page = session.get(f"{BASE_URL}/login")
    soup = BeautifulSoup(login_page.text, 'html.parser')
    csrf_input = soup.find('input', {'name': 'csrf_token'})
    csrf_token = csrf_input.get('value') if csrf_input else None
    
    if csrf_token:
        login_data = {
            "username": "testuser3",
            "password": "Password123!",
            "csrf_token": csrf_token
        }
        login_response = session.post(f"{BASE_URL}/login", data=login_data)
        print(f"Login status: {login_response.status_code}")
        
        # Now check upload form
        print("\n🔍 Checking upload form...")
        upload_page = session.get(f"{BASE_URL}/upload")
        print(f"Upload page status: {upload_page.status_code}")
        
        if upload_page.status_code == 200:
            soup = BeautifulSoup(upload_page.text, 'html.parser')
            
            # Look for all forms
            forms = soup.find_all('form')
            print(f"Found {len(forms)} forms on upload page")
            
            for i, form in enumerate(forms):
                print(f"\nForm {i+1}:")
                action = form.get('action', 'No action')
                method = form.get('method', 'GET')
                print(f"  Action: {action}")
                print(f"  Method: {method}")
                
                inputs = form.find_all(['input', 'select', 'textarea'])
                print(f"  Fields ({len(inputs)}):")
                for inp in inputs:
                    name = inp.get('name', 'unnamed')
                    input_type = inp.get('type', inp.name)
                    value = inp.get('value', '')
                    print(f"    - {name}: {input_type} = '{value[:50]}...' " if len(value) > 50 else f"    - {name}: {input_type} = '{value}'")
                    
            # Check for file inputs specifically
            file_inputs = soup.find_all('input', {'type': 'file'})
            print(f"\nFile inputs found: {len(file_inputs)}")
            for inp in file_inputs:
                name = inp.get('name', 'unnamed')
                print(f"  - {name}")
                
            # Check page content for clues
            if "login" in upload_page.text.lower() and "username" in upload_page.text.lower():
                print("\n⚠️  Upload page contains login form - authentication may have failed")
            elif "upload" in upload_page.text.lower():
                print("\n✅ Upload page contains upload-related content")
                
            # Save a snippet of the page for inspection
            print(f"\nFirst 1000 characters of upload page:")
            print(upload_page.text[:1000])
            
if __name__ == "__main__":
    debug_upload_form()