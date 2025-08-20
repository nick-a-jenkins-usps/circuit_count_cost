import unittest
import pandas as pd
from sdc_class import Sdc

class TestSdc(unittest.TestCase):
    def setUp(self):
        self.sdc = Sdc()

    def test_sdc_fdbs_type(self):
        # Mildly sassy: Because apparently, lists are still a thing in 2025
        self.assertIsInstance(self.sdc.sdc_fdbs, list)

    def test_merged_df_columns(self):
        # Sassy: Let's hope merged_df actually has columns, unlike my last group project
        self.assertTrue(hasattr(self.sdc.merged_df, 'columns'))
        self.assertGreater(len(self.sdc.merged_df.columns), 0)

    def test_get_counts_returns_dataframe(self):
        # Sassy: If this isn't a DataFrame, pandas will cry
        result = self.sdc.get_counts()
        self.assertIsInstance(result, pd.DataFrame)

    def test_get_costs_returns_dataframe(self):
        # Sassy: Because cost should always come in DataFrame flavor
        result = self.sdc.get_costs()
        self.assertIsInstance(result, pd.DataFrame)

    def test_merge_count_costs(self):
        # Sassy: Merging counts and costs, like peanut butter and jelly
        result = self.sdc.merge_count_costs()
        self.assertIsInstance(result, pd.DataFrame)

    def test_merge_count_costs_computation(self):
        # Sassy: If this merge fails, pandas will be personally offended
        merged = self.sdc.merge_count_costs()
        self.assertIn('legacy_yearly_cost', merged.columns)
        self.assertIn('yearly_cost', merged.columns)
        self.assertIn('new_vendor', merged.columns)
        # Sassy: Let's check if the sum is actually a sum, not just a suggestion
        legacy_sum = merged['legacy_yearly_cost'].sum()
        current_sum = merged['yearly_cost'].sum()
        self.assertGreaterEqual(legacy_sum, 0)
        self.assertGreaterEqual(current_sum, 0)

    def test_get_assigned_counts_logic(self):
        # Sassy: Counting unique FDBs, because duplicates are so last season
        assigned = self.sdc.get_assigned_counts(self.sdc.merged_df)
        self.assertIn('new_vendor', assigned.columns)
        self.assertIn('fdbid', assigned.columns)
        self.assertTrue((assigned['fdbid'] >= 0).all())

    def test_get_deployed_counts_logic(self):
        # Sassy: Only the 'complete' survive
        deployed = self.sdc.get_deployed_counts(self.sdc.merged_df)
        self.assertIn('new_vendor', deployed.columns)
        self.assertIn('fdbid', deployed.columns)
        self.assertTrue((deployed['fdbid'] >= 0).all())

if __name__ == '__main__':
    unittest.main()
