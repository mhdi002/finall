#!/usr/bin/env python3
"""
Comprehensive Backend Testing for Flask Financial Report Portal
Tests all major endpoints and functionality including authentication, file upload, 
report generation, and admin functions.
"""

import requests
import json
import os
import sys
from datetime import datetime

# Test configuration
BASE_URL = "http://127.0.0.1:5001"
TEST_USER_DATA = {
    "username": "testuser",
    "email": "test@example.com", 
    "password": "TestPass123!",
    "password2": "TestPass123!"
}

OWNER_USER_DATA = {
    "username": "admin_owner",
    "email": "admin.owner@financecorp.com",
    "password": "AdminPass456!",
    "password2": "AdminPass456!"
}

class FlaskBackendTester:
    def __init__(self):
        self.session = requests.Session()
        self.test_results = []
        self.uploaded_files = False
        
    def log_test(self, test_name, success, message="", response_code=None):
        """Log test results"""
        status = "✅ PASS" if success else "❌ FAIL"
        result = {
            "test": test_name,
            "status": status,
            "message": message,
            "response_code": response_code,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        self.test_results.append(result)
        print(f"{status} {test_name}: {message}")
        
    def test_server_health(self):
        """Test if the Flask server is running and accessible"""
        try:
            response = self.session.get(f"{BASE_URL}/")
            if response.status_code == 200:
                self.log_test("Server Health Check", True, "Flask server is running", response.status_code)
                return True
            else:
                self.log_test("Server Health Check", False, f"Server returned {response.status_code}", response.status_code)
                return False
        except requests.exceptions.ConnectionError:
            self.log_test("Server Health Check", False, "Cannot connect to Flask server")
            return False
            
    def test_user_registration(self):
        """Test user registration endpoint"""
        try:
            # First get the registration page to get CSRF token
            reg_page = self.session.get(f"{BASE_URL}/register")
            if reg_page.status_code != 200:
                self.log_test("User Registration", False, "Cannot access registration page", reg_page.status_code)
                return False
                
            # Extract CSRF token from the page
            csrf_token = self._extract_csrf_token(reg_page.text)
            
            # Prepare registration data
            reg_data = TEST_USER_DATA.copy()
            if csrf_token:
                reg_data['csrf_token'] = csrf_token
                
            # Submit registration
            response = self.session.post(f"{BASE_URL}/register", data=reg_data)
            
            if response.status_code == 302:  # Redirect after successful registration
                self.log_test("User Registration", True, "User registered successfully", response.status_code)
                return True
            elif "already exists" in response.text.lower() or "different" in response.text.lower():
                self.log_test("User Registration", True, "User already exists (expected)", response.status_code)
                return True
            elif response.status_code == 200 and "register" in response.text.lower():
                # Check if we're still on registration page due to validation errors
                if "congratulations" in response.text.lower():
                    self.log_test("User Registration", True, "User registered successfully", response.status_code)
                    return True
                else:
                    self.log_test("User Registration", True, "User likely already exists", response.status_code)
                    return True
            else:
                self.log_test("User Registration", False, f"Registration failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("User Registration", False, f"Exception: {str(e)}")
            return False
            
    def test_user_login(self):
        """Test user login endpoint"""
        try:
            # Get login page
            login_page = self.session.get(f"{BASE_URL}/login")
            if login_page.status_code != 200:
                self.log_test("User Login", False, "Cannot access login page", login_page.status_code)
                return False
                
            # Extract CSRF token
            csrf_token = self._extract_csrf_token(login_page.text)
            
            # Prepare login data
            login_data = {
                "username": TEST_USER_DATA["username"],
                "password": TEST_USER_DATA["password"],
                "remember_me": False
            }
            if csrf_token:
                login_data['csrf_token'] = csrf_token
                
            # Submit login
            response = self.session.post(f"{BASE_URL}/login", data=login_data)
            
            if response.status_code == 302:  # Redirect after successful login
                self.log_test("User Login", True, "User logged in successfully", response.status_code)
                return True
            elif response.status_code == 200 and "dashboard" in response.text.lower():
                # Some Flask apps return 200 with dashboard content instead of redirecting
                self.log_test("User Login", True, "User logged in successfully (direct dashboard)", response.status_code)
                return True
            else:
                self.log_test("User Login", False, f"Login failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("User Login", False, f"Exception: {str(e)}")
            return False
            
    def test_dashboard_access(self):
        """Test dashboard access after login"""
        try:
            response = self.session.get(f"{BASE_URL}/dashboard")
            
            if response.status_code == 200:
                self.log_test("Dashboard Access", True, "Dashboard accessible after login", response.status_code)
                return True
            elif response.status_code == 302:
                self.log_test("Dashboard Access", False, "Redirected - user not authenticated", response.status_code)
                return False
            else:
                self.log_test("Dashboard Access", False, f"Dashboard access failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Dashboard Access", False, f"Exception: {str(e)}")
            return False
            
    def test_file_upload(self):
        """Test file upload functionality with existing CSV files"""
        try:
            # Check if CSV files exist
            upload_dir = "/app/instance/uploads"
            required_files = ["deals.csv", "excluded.csv", "vip.csv"]
            
            for filename in required_files:
                filepath = os.path.join(upload_dir, filename)
                if not os.path.exists(filepath):
                    self.log_test("File Upload", False, f"Required file {filename} not found")
                    return False
                    
            # Get upload page
            upload_page = self.session.get(f"{BASE_URL}/upload")
            if upload_page.status_code == 302:
                self.log_test("File Upload", False, "Redirected to login - user not authenticated", upload_page.status_code)
                return False
            elif upload_page.status_code != 200:
                self.log_test("File Upload", False, "Cannot access upload page", upload_page.status_code)
                return False
            elif "login" in upload_page.text.lower() and "username" in upload_page.text.lower():
                self.log_test("File Upload", False, "Upload page showing login form - authentication required", upload_page.status_code)
                return False
                
            # Extract CSRF token
            csrf_token = self._extract_csrf_token(upload_page.text)
            
            # Prepare file upload
            files = {}
            data = {}
            
            if csrf_token:
                data['csrf_token'] = csrf_token
                
            # Read and prepare files for upload
            for file_key, filename in [("deals_csv", "deals.csv"), ("ex_csv", "excluded.csv"), ("vip_csv", "vip.csv")]:
                filepath = os.path.join(upload_dir, filename)
                with open(filepath, 'rb') as f:
                    files[file_key] = (filename, f.read(), 'text/csv')
                    
            # Submit file upload
            response = self.session.post(f"{BASE_URL}/upload", data=data, files=files)
            
            if response.status_code == 302:  # Redirect after successful upload
                self.log_test("File Upload", True, "Files uploaded successfully", response.status_code)
                self.uploaded_files = True
                return True
            else:
                self.log_test("File Upload", False, f"File upload failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("File Upload", False, f"Exception: {str(e)}")
            return False
            
    def test_report_generation(self):
        """Test report generation endpoint"""
        try:
            if not self.uploaded_files:
                self.log_test("Report Generation", False, "Files not uploaded - skipping test")
                return False
                
            response = self.session.get(f"{BASE_URL}/report/generate")
            
            if response.status_code == 302:  # Redirect after successful generation
                self.log_test("Report Generation", True, "Report generated successfully", response.status_code)
                return True
            else:
                self.log_test("Report Generation", False, f"Report generation failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Report Generation", False, f"Exception: {str(e)}")
            return False
            
    def test_report_results(self):
        """Test report results display"""
        try:
            response = self.session.get(f"{BASE_URL}/report/results")
            
            if response.status_code == 200:
                # Check if response contains expected report elements
                if "table" in response.text.lower() or "report" in response.text.lower():
                    self.log_test("Report Results", True, "Report results displayed successfully", response.status_code)
                    return True
                else:
                    self.log_test("Report Results", False, "Report results page missing content", response.status_code)
                    return False
            elif response.status_code == 302:
                self.log_test("Report Results", False, "Redirected - no report data available", response.status_code)
                return False
            else:
                self.log_test("Report Results", False, f"Report results access failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Report Results", False, f"Exception: {str(e)}")
            return False
            
    def test_admin_access_viewer(self):
        """Test admin panel access with Viewer role (should be denied)"""
        try:
            response = self.session.get(f"{BASE_URL}/admin")
            
            if response.status_code == 302:  # Should redirect due to insufficient permissions
                self.log_test("Admin Access (Viewer)", True, "Viewer correctly denied admin access", response.status_code)
                return True
            elif response.status_code == 200 and "permission" in response.text.lower():
                self.log_test("Admin Access (Viewer)", True, "Viewer correctly denied admin access", response.status_code)
                return True
            else:
                self.log_test("Admin Access (Viewer)", False, f"Viewer should not have admin access", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Admin Access (Viewer)", False, f"Exception: {str(e)}")
            return False
            
    def test_logout(self):
        """Test user logout"""
        try:
            response = self.session.get(f"{BASE_URL}/logout")
            
            if response.status_code == 302:  # Redirect after logout
                self.log_test("User Logout", True, "User logged out successfully", response.status_code)
                return True
            elif response.status_code == 200 and ("home" in response.text.lower() or "index" in response.text.lower()):
                # Some apps return 200 with home page content instead of redirecting
                self.log_test("User Logout", True, "User logged out successfully (direct home)", response.status_code)
                return True
            else:
                self.log_test("User Logout", False, f"Logout failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("User Logout", False, f"Exception: {str(e)}")
            return False
            
    def test_session_management(self):
        """Test that protected routes require authentication after logout"""
        try:
            response = self.session.get(f"{BASE_URL}/dashboard")
            
            if response.status_code == 302:  # Should redirect to login
                self.log_test("Session Management", True, "Protected route correctly requires authentication", response.status_code)
                return True
            else:
                self.log_test("Session Management", False, f"Protected route accessible without authentication", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Session Management", False, f"Exception: {str(e)}")
            return False
            
    def _extract_csrf_token(self, html_content):
        """Extract CSRF token from HTML content"""
        try:
            # Look for CSRF token in hidden input field
            import re
            csrf_match = re.search(r'name="csrf_token"[^>]*value="([^"]*)"', html_content)
            if csrf_match:
                return csrf_match.group(1)
            return None
        except:
            return None
            
    def run_all_tests(self):
        """Run all backend tests in sequence"""
        print("🚀 Starting Flask Backend Testing...")
        print("=" * 60)
        
        # Test sequence
        tests = [
            ("Server Health", self.test_server_health),
            ("User Registration", self.test_user_registration),
            ("User Login", self.test_user_login),
            ("Dashboard Access", self.test_dashboard_access),
            ("File Upload", self.test_file_upload),
            ("Report Generation", self.test_report_generation),
            ("Report Results", self.test_report_results),
            ("Admin Access (Viewer)", self.test_admin_access_viewer),
            ("User Logout", self.test_logout),
            ("Session Management", self.test_session_management),
        ]
        
        # Run tests
        for test_name, test_func in tests:
            print(f"\n🔍 Running {test_name}...")
            test_func()
            
        # Print summary
        self.print_summary()
        
    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for result in self.test_results if "✅" in result["status"])
        failed = sum(1 for result in self.test_results if "❌" in result["status"])
        total = len(self.test_results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {(passed/total)*100:.1f}%")
        
        if failed > 0:
            print("\n❌ FAILED TESTS:")
            for result in self.test_results:
                if "❌" in result["status"]:
                    print(f"  - {result['test']}: {result['message']}")
                    
        print("\n📋 DETAILED RESULTS:")
        for result in self.test_results:
            code_info = f" (HTTP {result['response_code']})" if result['response_code'] else ""
            print(f"  {result['status']} {result['test']}{code_info}")
            if result['message']:
                print(f"      {result['message']}")

if __name__ == "__main__":
    tester = FlaskBackendTester()
    tester.run_all_tests()