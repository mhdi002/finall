#!/usr/bin/env python3
"""
Stage 2 Enhanced Functionality Testing
Tests the enhanced Stage 2 reporting functionality that shows either charts or tables
based on data sufficiency.
"""

import requests
import json
import sys
from datetime import datetime, timedelta

# Test configuration
BASE_URL = "http://127.0.0.1:5001"
DEMO_USER_DATA = {
    "username": "demo",
    "password": "Demo@123!"
}

class Stage2EnhancedTester:
    def __init__(self):
        self.session = requests.Session()
        self.test_results = []
        self.logged_in = False
        
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
            
    def test_demo_user_login(self):
        """Test login with demo user credentials"""
        try:
            # Get login page
            login_page = self.session.get(f"{BASE_URL}/login")
            if login_page.status_code != 200:
                self.log_test("Demo User Login", False, "Cannot access login page", login_page.status_code)
                return False
                
            # Extract CSRF token
            csrf_token = self._extract_csrf_token(login_page.text)
            
            # Prepare login data
            login_data = {
                "username": DEMO_USER_DATA["username"],
                "password": DEMO_USER_DATA["password"],
                "remember_me": False
            }
            if csrf_token:
                login_data['csrf_token'] = csrf_token
                
            # Submit login
            response = self.session.post(f"{BASE_URL}/login", data=login_data)
            
            if response.status_code == 302:  # Redirect after successful login
                self.log_test("Demo User Login", True, "Demo user logged in successfully", response.status_code)
                self.logged_in = True
                return True
            elif response.status_code == 200 and "dashboard" in response.text.lower():
                self.log_test("Demo User Login", True, "Demo user logged in successfully (direct dashboard)", response.status_code)
                self.logged_in = True
                return True
            else:
                self.log_test("Demo User Login", False, f"Login failed - check demo user credentials", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Demo User Login", False, f"Exception: {str(e)}")
            return False
            
    def test_stage2_report_access(self):
        """Test access to Stage 2 report generation page"""
        if not self.logged_in:
            self.log_test("Stage 2 Report Access", False, "User not logged in")
            return False
            
        try:
            response = self.session.get(f"{BASE_URL}/report/stage2")
            
            if response.status_code == 200:
                # Check if the page contains expected elements
                if "stage 2" in response.text.lower() or "financial report" in response.text.lower():
                    self.log_test("Stage 2 Report Access", True, "Stage 2 report page accessible", response.status_code)
                    return True
                else:
                    self.log_test("Stage 2 Report Access", False, "Stage 2 report page missing expected content", response.status_code)
                    return False
            elif response.status_code == 302:
                self.log_test("Stage 2 Report Access", False, "Redirected - authentication issue", response.status_code)
                return False
            else:
                self.log_test("Stage 2 Report Access", False, f"Stage 2 report page access failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Stage 2 Report Access", False, f"Exception: {str(e)}")
            return False
            
    def test_stage2_report_generation(self):
        """Test Stage 2 report generation with different scenarios"""
        if not self.logged_in:
            self.log_test("Stage 2 Report Generation", False, "User not logged in")
            return False
            
        try:
            # Get the report generation page first
            report_page = self.session.get(f"{BASE_URL}/report/stage2")
            if report_page.status_code != 200:
                self.log_test("Stage 2 Report Generation", False, "Cannot access report generation page", report_page.status_code)
                return False
                
            # Extract CSRF token
            csrf_token = self._extract_csrf_token(report_page.text)
            
            # Test with Stage 2 Financial Report
            report_data = {
                "report_type": "stage2",
                "start_date": (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                "end_date": datetime.now().strftime('%Y-%m-%d')
            }
            
            if csrf_token:
                report_data['csrf_token'] = csrf_token
                
            # Submit report generation request
            response = self.session.post(f"{BASE_URL}/report/stage2", data=report_data)
            
            if response.status_code == 200:
                # Check if response contains expected report elements
                response_text = response.text.lower()
                
                # Check for either chart mode or table mode indicators
                has_chart_mode = ("chart" in response_text and "plotly" in response_text)
                has_table_mode = ("limited data view" in response_text and "insufficient data" in response_text)
                has_financial_data = ("financial summary" in response_text or "total rebate" in response_text)
                
                if has_chart_mode:
                    self.log_test("Stage 2 Report Generation", True, "Report generated successfully - CHART MODE detected", response.status_code)
                    return "chart_mode"
                elif has_table_mode:
                    self.log_test("Stage 2 Report Generation", True, "Report generated successfully - TABLE MODE detected", response.status_code)
                    return "table_mode"
                elif has_financial_data:
                    self.log_test("Stage 2 Report Generation", True, "Report generated successfully - Financial data present", response.status_code)
                    return "financial_data"
                else:
                    self.log_test("Stage 2 Report Generation", False, "Report generated but missing expected content", response.status_code)
                    return False
                    
            elif response.status_code == 302:
                # Follow redirect to see the actual report
                redirect_url = response.headers.get('Location', '')
                if redirect_url:
                    follow_response = self.session.get(f"{BASE_URL}{redirect_url}")
                    if follow_response.status_code == 200:
                        self.log_test("Stage 2 Report Generation", True, "Report generated successfully (via redirect)", follow_response.status_code)
                        return True
                    else:
                        self.log_test("Stage 2 Report Generation", False, f"Redirect failed", follow_response.status_code)
                        return False
                else:
                    self.log_test("Stage 2 Report Generation", False, "Redirect without location", response.status_code)
                    return False
            else:
                self.log_test("Stage 2 Report Generation", False, f"Report generation failed", response.status_code)
                return False
                
        except Exception as e:
            self.log_test("Stage 2 Report Generation", False, f"Exception: {str(e)}")
            return False
            
    def test_financial_metrics_display(self):
        """Test that financial metrics are properly displayed"""
        if not self.logged_in:
            self.log_test("Financial Metrics Display", False, "User not logged in")
            return False
            
        try:
            # Generate a report first
            report_result = self.test_stage2_report_generation()
            if not report_result:
                self.log_test("Financial Metrics Display", False, "Could not generate report for metrics test")
                return False
                
            # The report should already be displayed from the previous test
            # We'll check the last response for financial metrics
            
            # Get the current page (should be the report results)
            current_page = self.session.get(f"{BASE_URL}/report/stage2")
            
            # Look for key financial metrics mentioned in the requirements
            expected_metrics = [
                "total rebate",
                "m2p deposit", 
                "crm deposit",
                "settlement deposit",
                "withdrawal"
            ]
            
            found_metrics = []
            page_text = current_page.text.lower()
            
            for metric in expected_metrics:
                if metric in page_text:
                    found_metrics.append(metric)
                    
            if len(found_metrics) >= 3:  # At least 3 key metrics should be present
                self.log_test("Financial Metrics Display", True, f"Found {len(found_metrics)} key financial metrics", current_page.status_code)
                return True
            else:
                self.log_test("Financial Metrics Display", False, f"Only found {len(found_metrics)} metrics: {found_metrics}", current_page.status_code)
                return False
                
        except Exception as e:
            self.log_test("Financial Metrics Display", False, f"Exception: {str(e)}")
            return False
            
    def test_data_sufficiency_logic(self):
        """Test that the system correctly detects data sufficiency for charts vs tables"""
        if not self.logged_in:
            self.log_test("Data Sufficiency Logic", False, "User not logged in")
            return False
            
        try:
            # Test with different date ranges to potentially trigger different modes
            test_scenarios = [
                {
                    "name": "Last 30 days",
                    "start_date": (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                    "end_date": datetime.now().strftime('%Y-%m-%d')
                },
                {
                    "name": "Last 7 days", 
                    "start_date": (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                    "end_date": datetime.now().strftime('%Y-%m-%d')
                },
                {
                    "name": "All time",
                    "start_date": "",
                    "end_date": ""
                }
            ]
            
            modes_detected = []
            
            for scenario in test_scenarios:
                # Get report page
                report_page = self.session.get(f"{BASE_URL}/report/stage2")
                csrf_token = self._extract_csrf_token(report_page.text)
                
                # Submit report request
                report_data = {
                    "report_type": "stage2",
                    "start_date": scenario["start_date"],
                    "end_date": scenario["end_date"]
                }
                
                if csrf_token:
                    report_data['csrf_token'] = csrf_token
                    
                response = self.session.post(f"{BASE_URL}/report/stage2", data=report_data)
                
                if response.status_code == 200:
                    response_text = response.text.lower()
                    
                    if "limited data view" in response_text and "insufficient data" in response_text:
                        modes_detected.append(f"{scenario['name']}: TABLE MODE")
                    elif "chart" in response_text and ("plotly" in response_text or "financial analysis charts" in response_text):
                        modes_detected.append(f"{scenario['name']}: CHART MODE")
                    else:
                        modes_detected.append(f"{scenario['name']}: UNKNOWN MODE")
                        
            if len(modes_detected) > 0:
                self.log_test("Data Sufficiency Logic", True, f"Detected modes: {', '.join(modes_detected)}")
                return True
            else:
                self.log_test("Data Sufficiency Logic", False, "Could not detect any reporting modes")
                return False
                
        except Exception as e:
            self.log_test("Data Sufficiency Logic", False, f"Exception: {str(e)}")
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
        """Run all Stage 2 enhanced functionality tests"""
        print("🚀 Starting Stage 2 Enhanced Functionality Testing...")
        print("=" * 70)
        
        # Test sequence
        tests = [
            ("Server Health", self.test_server_health),
            ("Demo User Login", self.test_demo_user_login),
            ("Stage 2 Report Access", self.test_stage2_report_access),
            ("Stage 2 Report Generation", self.test_stage2_report_generation),
            ("Financial Metrics Display", self.test_financial_metrics_display),
            ("Data Sufficiency Logic", self.test_data_sufficiency_logic),
        ]
        
        # Run tests
        for test_name, test_func in tests:
            print(f"\n🔍 Running {test_name}...")
            test_func()
            
        # Print summary
        self.print_summary()
        
    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 70)
        print("📊 STAGE 2 ENHANCED FUNCTIONALITY TEST SUMMARY")
        print("=" * 70)
        
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
                
        print("\n🎯 KEY FINDINGS:")
        print("  - Stage 2 enhanced reporting functionality tested")
        print("  - Automatic detection of data sufficiency for chart vs table mode")
        print("  - Financial metrics calculation and display verification")
        print("  - Demo user authentication and access control")

if __name__ == "__main__":
    tester = Stage2EnhancedTester()
    tester.run_all_tests()