import unittest
import pandas as pd
import os
from app.processing import run_report_processing

class TestEndToEndProcessing(unittest.TestCase):

    def test_run_with_provided_csv(self):
        """
        Test the entire report processing pipeline with the provided CSV file.
        """
        # Define file paths
        deals_path = 'All_Pages_Deals_MT5 (1).csv'
        excluded_path = 'tests/data/excluded.csv'
        vip_path = 'tests/data/vip.csv'

        # Check if the main deals file exists
        self.assertTrue(os.path.exists(deals_path), f"Deals file not found at {deals_path}")

        # Load dataframes
        deals_df = pd.read_csv(deals_path)
        try:
            excluded_df = pd.read_csv(excluded_path, header=None)
        except pd.errors.EmptyDataError:
            excluded_df = pd.DataFrame()

        try:
            vip_df = pd.read_csv(vip_path, header=None)
        except pd.errors.EmptyDataError:
            vip_df = pd.DataFrame()

        # Run the processing function
        try:
            results = run_report_processing(deals_df, excluded_df, vip_df)
        except Exception as e:
            self.fail(f"run_report_processing raised an exception: {e}")

        # Assertions to verify the output
        self.assertIsInstance(results, dict)
        self.assertTrue(len(results) > 0, "The results dictionary should not be empty")

        expected_keys = [
            "A Book Raw", "B Book Raw", "Multi Book Raw",
            "A Book Result", "B Book Result", "Multi Book Result",
            "Chinese Clients", "Client Summary", "Final Calculations",
            "VIP Volume"
        ]

        for key in expected_keys:
            self.assertIn(key, results, f"Expected key '{key}' not found in results")
            if key != "VIP Volume":
                self.assertIsInstance(results[key], pd.DataFrame, f"Value for key '{key}' should be a DataFrame")

        # Check that the results are not all empty
        self.assertFalse(results["A Book Result"].empty, "A Book Result should not be empty")
        self.assertFalse(results["B Book Result"].empty, "B Book Result should not be empty")
        self.assertFalse(results["Final Calculations"].empty, "Final Calculations should not be empty")

if __name__ == '__main__':
    unittest.main()
