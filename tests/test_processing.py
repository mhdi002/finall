import unittest
import pandas as pd
from app.processing import run_report_processing, aggregate_book, generate_final_calculations

class TestReportProcessing(unittest.TestCase):

    def setUp(self):
        """Set up sample dataframes for tests."""
        self.deals_data = {
            'Deal': [101, 102, 103, 104, 105, 106, 107],
            'Login': [1001, 1002, 1001, 1003, 1004, 1005, 1002],
            'Group': ['real\\Retail', 'real\\Chines', 'real\\Retail', 'BBOOK\\Retail', 'BBOOK\\Chines', 'real\\Excluded', 'real\\Chines'],
            'Processing rule': ['Pipwise', 'Pipwise', 'Multi Book', 'Retail B-book', 'Retail B-book', 'Pipwise', 'Pipwise'],
            'Notional volume in USD': [10000, 20000, 5000, 15000, 25000, 50000, 8000],
            'Trader profit': [100, -50, 20, -30, 150, 200, 40],
            'Swaps': [10, 5, 2, -5, 15, 20, 3],
            'Commission': [5, 10, 3, 8, 12, 25, 4],
            'TP broker profit': [50, 60, 10, 0, 0, 100, 20],
            'Total broker profit': [50, 60, 10, 90, 80, 100, 20],
            'Date & Time (UTC)': ['01.01.2024 10:00:00']*7,
            'Profit': ['105.00 USD', '-55.00 USC', '22.00 USD', '-35.00 USD', '165.00 USD', '220.00 USD', '43.00 USD'],
        }
        self.deals_df = pd.DataFrame(self.deals_data)

        self.excluded_df = pd.DataFrame([1005]) # Excluded Login
        self.vip_df = pd.DataFrame([1002])      # VIP Login

    def test_full_run(self):
        """Test the main orchestrator function `run_report_processing`."""
        results = run_report_processing(self.deals_df, self.excluded_df, self.vip_df)
        self.assertIsInstance(results, dict)
        self.assertIn('Final Calculations', results)

        final_calcs = results['Final Calculations'].set_index('Source')['Value']

        # Based on the sample data:
        # A Book: Login 1001 (Deal 101), 1002 (Deals 102, 107). Login 1005 is excluded.
        #   - Vol: 10000 + 20000 + 8000 = 38000
        #   - Comm: 5 + 10 + 4 = 19
        #   - TP Profit: 50 + 60 + 20 = 130
        # Multi Book: Login 1001 (Deal 103)
        #   - Vol: 5000
        #   - Comm: 3
        #   - TP Profit: 10
        # B Book: Login 1003, 1004
        #   - Vol: 15000 + 25000 = 40000

        # Total A Book = (A Book Comm + A Book TP) + (Multi Book Comm + Multi Book TP)
        # (19 + 130) + (3 + 10) = 149 + 13 = 162
        self.assertAlmostEqual(float(final_calcs['Total A Book']), 162.0)

        # Total B Book TSM = -1 * (Trader + Swaps - Comm)
        # Login 1003: -30 + -5 - 8 = -43
        # Login 1004: 150 + 15 - 12 = 153
        # Total Net = -43 + 153 = 110. TSM = -110
        # Total B Book = TSM = -110 (since no Multi Book broker profit diff)
        self.assertAlmostEqual(float(final_calcs['Total B Book']), -110.0)

        # Total Volume (Lot) = (A Vol + B Vol + Multi Vol) / 200000
        # (88000 + 40000 + 5000) / 200000 = 133000 / 200000 = 0.665
        self.assertAlmostEqual(float(final_calcs['Total Volume']), 0.665)

    def test_a_book_exclusion(self):
        """Test that excluded accounts in A-Book have their profits/commissions zeroed out."""
        a_book_data = {
            'Login': [1001, 1005], # 1005 is excluded
            'Notional volume in USD': [10000, 50000],
            'Trader profit': [100, 200],
            'Swaps': [10, 20],
            'Commission': [5, 25],
            'TP broker profit': [50, 100],
            'Total broker profit': [50, 100],
        }
        a_book_df = pd.DataFrame(a_book_data)
        excluded_set = {str(1005)}

        agg_result = aggregate_book(a_book_df, excluded_set, "A Book")

        # Check excluded user 1005
        excluded_row = agg_result[agg_result['Login'] == '1005'].iloc[0]
        self.assertEqual(excluded_row['Commission'], 0)
        self.assertEqual(excluded_row['TP Profit'], 0)
        self.assertEqual(excluded_row['Broker Profit'], 0)
        # Volume and other fields should remain
        self.assertEqual(excluded_row['Total Volume'], 50000)

        # Check non-excluded user 1001
        included_row = agg_result[agg_result['Login'] == '1001'].iloc[0]
        self.assertEqual(included_row['Commission'], 5)
        self.assertEqual(included_row['TP Profit'], 50)

    def test_b_book_exclusion(self):
        """Test that excluded accounts are skipped entirely in B-Book."""
        b_book_data = {
            'Login': [1003, 1005], # 1005 is excluded
            'Notional volume in USD': [15000, 50000],
            'Trader profit': [-30, 200],
            'Swaps': [-5, 20],
            'Commission': [8, 25],
            'TP broker profit': [0, 100],
            'Total broker profit': [90, 100],
        }
        b_book_df = pd.DataFrame(b_book_data)
        excluded_set = {str(1005)}

        agg_result = aggregate_book(b_book_df, excluded_set, "B Book")

        # Excluded user 1005 should not be in the result at all
        self.assertTrue('1005' not in agg_result['Login'].values)
        # There should be 2 rows: user 1003 and the Summary row
        self.assertEqual(len(agg_result), 2)

if __name__ == '__main__':
    unittest.main()
