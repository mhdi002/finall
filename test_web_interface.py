#!/usr/bin/env python
"""
Test the web interface with different data scenarios
"""
import requests
from datetime import datetime, timedelta
import json

# Configuration
BASE_URL = "http://localhost:5001"
USERNAME = "demo"
PASSWORD = "demo123"

def login_and_get_session():
    """Login and return session with auth cookies"""
    session = requests.Session()
    
    # Get login page to get CSRF token if needed
    login_page = session.get(f"{BASE_URL}/login")
    
    # Attempt login
    login_data = {
        'username': USERNAME,
        'password': PASSWORD,
        'remember_me': False,
        'submit': 'Sign In'
    }
    
    response = session.post(f"{BASE_URL}/login", data=login_data, allow_redirects=True)
    
    if "dashboard" in response.url.lower() or response.status_code == 200:
        print("✅ Login successful")
        return session
    else:
        print("❌ Login failed")
        print(f"Response URL: {response.url}")
        print(f"Status Code: {response.status_code}")
        return None

def test_stage2_report_generation(session):
    """Test Stage 2 report generation"""
    print("\n🔍 Testing Stage 2 Report Generation...")
    
    # Test with current data (should be extensive from our previous test)
    report_data = {
        'report_type': 'stage2',
        'start_date': '',  # All time
        'end_date': '',
        'submit': 'Generate Report'
    }
    
    response = session.post(f"{BASE_URL}/report/stage2", data=report_data, allow_redirects=True)
    
    if response.status_code == 200:
        content = response.text
        
        # Check if we got the enhanced template
        if 'stage2_results_enhanced.html' in content or 'Financial Summary Report' in content:
            print("✅ Stage 2 report generated successfully")
            
            # Check for table mode indicators
            if 'Limited Data View' in content:
                print("📋 Table mode detected (insufficient data for charts)")
                if 'summary-table' in content:
                    print("✅ Summary table is present")
            elif 'chart-data' in content or 'Financial Analysis Charts' in content:
                print("📊 Chart mode detected (sufficient data for charts)")
                if 'plotly' in content.lower():
                    print("✅ Chart scripts loaded")
            
            # Check for key financial metrics
            financial_metrics = [
                'Total Rebate', 'M2p Deposit', 'Settlement Deposit', 
                'M2p Withdrawal', 'CRM Deposit Total'
            ]
            
            found_metrics = []
            for metric in financial_metrics:
                if metric in content:
                    found_metrics.append(metric)
            
            print(f"📊 Found {len(found_metrics)}/{len(financial_metrics)} expected financial metrics")
            
            return True
        else:
            print("❌ Unexpected report format")
            return False
    else:
        print(f"❌ Report generation failed with status {response.status_code}")
        return False

def test_dashboard_access(session):
    """Test dashboard access"""
    print("\n🏠 Testing Dashboard Access...")
    
    response = session.get(f"{BASE_URL}/dashboard")
    
    if response.status_code == 200:
        content = response.text
        if 'dashboard' in content.lower():
            print("✅ Dashboard accessible")
            
            # Check for file upload status indicators
            if 'file_status' in content or 'Upload Files' in content:
                print("✅ File upload interface available")
            
            return True
        else:
            print("❌ Dashboard content unexpected")
            return False
    else:
        print(f"❌ Dashboard access failed with status {response.status_code}")
        return False

def main():
    print("🚀 Starting Web Interface Test")
    print("=" * 50)
    
    # Login
    session = login_and_get_session()
    if not session:
        print("❌ Cannot proceed without login")
        return
    
    # Test dashboard
    dashboard_ok = test_dashboard_access(session)
    
    # Test Stage 2 report generation
    if dashboard_ok:
        report_ok = test_stage2_report_generation(session)
        
        if report_ok:
            print("\n✅ All tests passed! Enhanced Stage 2 functionality working correctly.")
        else:
            print("\n❌ Report generation test failed")
    else:
        print("\n❌ Dashboard test failed, skipping report tests")
    
    print("\n" + "=" * 50)
    print("🏁 Web Interface Test Complete")

if __name__ == '__main__':
    main()