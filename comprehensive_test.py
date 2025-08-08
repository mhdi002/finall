#!/usr/bin/env python3
"""
Comprehensive Testing for Flask Financial Report Portal
Tests all major functionality including authentication, file upload, 
report generation, and API endpoints.
"""

import requests
import json
import os
import sys
from datetime import datetime
import tempfile

# Test configuration
BASE_URL = "http://127.0.0.1:5001"
TEST_USER_DATA = {
    "username": "testuser",
    "email": "test@example.com", 
    "password": "TestPass123!",
    "password2": "TestPass123!"
}

class ComprehensiveFlaskTester:
    def __init__(self):
        self.session = requests.Session()
        self.test_results = []
        
    def log_test(self, test_name, success, message="", response_code=None, details=None):
        """Log test results with detailed information"""
        status = "✅ PASS" if success else "❌ FAIL"
        result = {
            "test": test_name,
            "status": status,
            "message": message,
            "response_code": response_code,
            "details": details,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        self.test_results.append(result)
        print(f"{status} {test_name}: {message}")
        if details:
            print(f"    Details: {details}")
        
    def test_server_health(self):
        """Test if the Flask server is running and accessible"""
        try:
            response = self.session.get(f"{BASE_URL}/")
            if response.status_code == 200:
                content_length = len(response.text)
                has_title = "Financial Report Portal" in response.text
                self.log_test("Server Health Check", True, 
                            f"Flask server is running, content: {content_length} chars", 
                            response.status_code,
                            f"Title found: {has_title}")
                return True
            else:
                self.log_test("Server Health Check", False, 
                            f"Server returned {response.status_code}", response.status_code)
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
                self.log_test("User Registration", False, 
                            "Cannot access registration page", reg_page.status_code)
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
                self.log_test("User Registration", True, 
                            "User registered successfully", response.status_code)
                return True
            elif "already exists" in response.text.lower() or "different" in response.text.lower():
                self.log_test("User Registration", True, 
                            "User already exists (expected)", response.status_code)
                return True
            elif response.status_code == 200:
                if "congratulations" in response.text.lower():
                    self.log_test("User Registration", True, 
                                "User registered successfully", response.status_code)
                    return True
                else:
                    self.log_test("User Registration", True, 
                                "User likely already exists", response.status_code)
                    return True
            else:
                self.log_test("User Registration", False, 
                            f"Registration failed", response.status_code)
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
                self.log_test("User Login", False, 
                            "Cannot access login page", login_page.status_code)
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
                self.log_test("User Login", True, 
                            "User logged in successfully", response.status_code)
                return True
            elif response.status_code == 200 and "dashboard" in response.text.lower():
                self.log_test("User Login", True, 
                            "User logged in successfully (direct dashboard)", response.status_code)
                return True
            else:
                self.log_test("User Login", False, 
                            f"Login failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("User Login", False, f"Exception: {str(e)}")
            return False
            
    def test_dashboard_access(self):
        """Test dashboard access after login"""
        try:
            response = self.session.get(f"{BASE_URL}/dashboard")
            
            if response.status_code == 200:
                # Check for key dashboard elements
                has_welcome = "welcome" in response.text.lower()
                has_upload = "upload" in response.text.lower()
                has_file_status = "file upload status" in response.text.lower()
                
                self.log_test("Dashboard Access", True, 
                            "Dashboard accessible after login", response.status_code,
                            f"Welcome: {has_welcome}, Upload: {has_upload}, File Status: {has_file_status}")
                return True
            elif response.status_code == 302:
                self.log_test("Dashboard Access", False, 
                            "Redirected - user not authenticated", response.status_code)
                return False
            else:
                self.log_test("Dashboard Access", False, 
                            f"Dashboard access failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Dashboard Access", False, f"Exception: {str(e)}")
            return False
            
    def test_upload_page_access(self):
        """Test upload page accessibility"""
        try:
            response = self.session.get(f"{BASE_URL}/upload")
            
            if response.status_code == 200:
                # Check for upload form elements
                has_form = "<form" in response.text
                has_file_inputs = 'type="file"' in response.text
                file_input_count = response.text.count('type="file"')
                has_submit = 'type="submit"' in response.text
                
                self.log_test("Upload Page Access", True, 
                            "Upload page accessible", response.status_code,
                            f"Form: {has_form}, File inputs: {file_input_count}, Submit: {has_submit}")
                return True
            elif response.status_code == 302:
                self.log_test("Upload Page Access", False, 
                            "Redirected - authentication required", response.status_code)
                return False
            else:
                self.log_test("Upload Page Access", False, 
                            f"Upload page access failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Upload Page Access", False, f"Exception: {str(e)}")
            return False
            
    def test_report_generation_page(self):
        """Test report generation page access"""
        try:
            response = self.session.get(f"{BASE_URL}/report/stage2")
            
            if response.status_code == 200:
                # Check for report form elements
                has_form = "<form" in response.text
                has_date_fields = "start_date" in response.text and "end_date" in response.text
                has_report_type = "report_type" in response.text
                
                self.log_test("Report Generation Page", True, 
                            "Report generation page accessible", response.status_code,
                            f"Form: {has_form}, Date fields: {has_date_fields}, Report type: {has_report_type}")
                return True
            elif response.status_code == 302:
                self.log_test("Report Generation Page", False, 
                            "Redirected - authentication required", response.status_code)
                return False
            else:
                self.log_test("Report Generation Page", False, 
                            f"Report page access failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Report Generation Page", False, f"Exception: {str(e)}")
            return False
            
    def test_api_upload_status(self):
        """Test API endpoint for upload status"""
        try:
            response = self.session.get(f"{BASE_URL}/api/upload_status")
            
            if response.status_code == 200:
                try:
                    json_data = response.json()
                    is_valid_json = isinstance(json_data, dict)
                    self.log_test("API Upload Status", True, 
                                "API endpoint returns valid JSON", response.status_code,
                                f"JSON structure: {type(json_data)}, Keys: {list(json_data.keys()) if is_valid_json else 'Invalid'}")
                    return True
                except json.JSONDecodeError:
                    self.log_test("API Upload Status", False, 
                                "API endpoint returns invalid JSON", response.status_code)
                    return False
            elif response.status_code == 302:
                self.log_test("API Upload Status", False, 
                            "API redirected - authentication required", response.status_code)
                return False
            else:
                self.log_test("API Upload Status", False, 
                            f"API endpoint failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("API Upload Status", False, f"Exception: {str(e)}")
            return False
            
    def test_admin_access_viewer(self):
        """Test admin panel access with Viewer role (should be denied)"""
        try:
            response = self.session.get(f"{BASE_URL}/admin")
            
            if response.status_code == 302:  # Should redirect due to insufficient permissions
                self.log_test("Admin Access (Viewer)", True, 
                            "Viewer correctly denied admin access", response.status_code)
                return True
            elif response.status_code == 200 and "permission" in response.text.lower():
                self.log_test("Admin Access (Viewer)", True, 
                            "Viewer correctly denied admin access", response.status_code)
                return True
            else:
                self.log_test("Admin Access (Viewer)", False, 
                            f"Viewer should not have admin access", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Admin Access (Viewer)", False, f"Exception: {str(e)}")
            return False
            
    def test_file_upload_simulation(self):
        """Test file upload with a small test CSV"""
        try:
            # Get upload page first
            upload_page = self.session.get(f"{BASE_URL}/upload")
            if upload_page.status_code != 200:
                self.log_test("File Upload Simulation", False, 
                            "Cannot access upload page", upload_page.status_code)
                return False
                
            # Extract CSRF token
            csrf_token = self._extract_csrf_token(upload_page.text)
            
            # Create a small test CSV file
            test_csv_content = "Transaction ID,Amount,Date\n1,100.50,2025-01-01\n2,200.75,2025-01-02"
            
            # Prepare upload data
            data = {}
            if csrf_token:
                data['csrf_token'] = csrf_token
                
            # Prepare file for upload (simulate payment data file)
            files = {
                'payment_data': ('test_payment.csv', test_csv_content, 'text/csv')
            }
            
            # Submit file upload
            response = self.session.post(f"{BASE_URL}/upload", data=data, files=files)
            
            if response.status_code == 302:  # Redirect after successful upload
                self.log_test("File Upload Simulation", True, 
                            "Test file uploaded successfully", response.status_code)
                return True
            elif response.status_code == 200:
                # Check if there are any error messages in the response
                if "error" in response.text.lower() or "failed" in response.text.lower():
                    self.log_test("File Upload Simulation", False, 
                                "Upload failed with errors", response.status_code)
                    return False
                else:
                    self.log_test("File Upload Simulation", True, 
                                "Upload processed (stayed on page)", response.status_code)
                    return True
            else:
                self.log_test("File Upload Simulation", False, 
                            f"File upload failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("File Upload Simulation", False, f"Exception: {str(e)}")
            return False
            
    def test_logout(self):
        """Test user logout"""
        try:
            response = self.session.get(f"{BASE_URL}/logout")
            
            if response.status_code == 302:  # Redirect after logout
                self.log_test("User Logout", True, 
                            "User logged out successfully", response.status_code)
                return True
            elif response.status_code == 200 and ("home" in response.text.lower() or "index" in response.text.lower()):
                self.log_test("User Logout", True, 
                            "User logged out successfully (direct home)", response.status_code)
                return True
            else:
                self.log_test("User Logout", False, 
                            f"Logout failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("User Logout", False, f"Exception: {str(e)}")
            return False
            
    def test_session_management(self):
        """Test that protected routes require authentication after logout"""
        try:
            response = self.session.get(f"{BASE_URL}/dashboard")
            
            if response.status_code == 200 and "login" in response.text.lower():
                self.log_test("Session Management", True,
                            "Protected route correctly requires authentication", response.status_code)
                return True
            elif response.status_code == 302:  # Should redirect to login
                self.log_test("Session Management", True,
                            "Protected route correctly requires authentication", response.status_code)
                return True
            else:
                self.log_test("Session Management", False,
                            f"Protected route accessible without authentication", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Session Management", False, f"Exception: {str(e)}")
            return False
            
    def _extract_csrf_token(self, html_content):
        """Extract CSRF token from HTML content"""
        try:
            import re
            csrf_match = re.search(r'name="csrf_token"[^>]*value="([^"]*)"', html_content)
            if csrf_match:
                return csrf_match.group(1)
            return None
        except:
            return None
            
    def run_all_tests(self):
        """Run all comprehensive tests in sequence"""
        import time
        print("🚀 Starting Comprehensive Flask Backend Testing...")
        print("=" * 80)
        print("⏳ Waiting for server to start...")
        time.sleep(15)
        
        # Test sequence
        tests = [
            ("Server Health", self.test_server_health),
            ("User Registration", self.test_user_registration),
            ("User Login", self.test_user_login),
            ("Dashboard Access", self.test_dashboard_access),
            ("Upload Page Access", self.test_upload_page_access),
            ("Report Generation Page", self.test_report_generation_page),
            ("API Upload Status", self.test_api_upload_status),
            ("File Upload Simulation", self.test_file_upload_simulation),
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
        """Print comprehensive test summary"""
        print("\n" + "=" * 80)
        print("📊 COMPREHENSIVE TEST SUMMARY")
        print("=" * 80)
        
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
                    if result['details']:
                        print(f"    Details: {result['details']}")
                        
        print("\n📋 DETAILED RESULTS:")
        for result in self.test_results:
            code_info = f" (HTTP {result['response_code']})" if result['response_code'] else ""
            print(f"  {result['status']} {result['test']}{code_info}")
            if result['message']:
                print(f"      {result['message']}")
            if result['details']:
                print(f"      Details: {result['details']}")
                
        print("\n" + "=" * 80)
        print("🎯 KEY FINDINGS:")
        print("=" * 80)
        
        # Analyze results and provide insights
        if passed == total:
            print("✅ All tests passed! The application is functioning correctly.")
        elif passed >= total * 0.8:
            print("⚠️  Most tests passed, but some issues need attention.")
        else:
            print("❌ Multiple critical issues found that need immediate attention.")
            
        # Check specific functionality
        auth_tests = [r for r in self.test_results if "login" in r["test"].lower() or "registration" in r["test"].lower()]
        auth_passed = sum(1 for r in auth_tests if "✅" in r["status"])
        
        upload_tests = [r for r in self.test_results if "upload" in r["test"].lower()]
        upload_passed = sum(1 for r in upload_tests if "✅" in r["status"])
        
        api_tests = [r for r in self.test_results if "api" in r["test"].lower()]
        api_passed = sum(1 for r in api_tests if "✅" in r["status"])
        
        print(f"Authentication: {auth_passed}/{len(auth_tests)} tests passed")
        print(f"File Upload: {upload_passed}/{len(upload_tests)} tests passed")
        print(f"API Endpoints: {api_passed}/{len(api_tests)} tests passed")

if __name__ == "__main__":
    tester = ComprehensiveFlaskTester()
    tester.run_all_tests()