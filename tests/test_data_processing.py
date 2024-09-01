# test_data_processing.py
import unittest
import pandas as pd
from unittest.mock import patch
from data_processing.transformer import (
    calculate_issue_resolution_time,
    calculate_pr_merge_time,
    aggregate_repository_metrics,
    calculate_contributor_activity,
    transform_all_data
)

class TestTransformer(unittest.TestCase):
    def setUp(self):
        self.sample_repo_data = pd.DataFrame({
            'id': [1],
            'full_name': ['user/repo1'],
            'stars': [100],
            'forks': [50],
        })

        self.sample_issues_data = pd.DataFrame({
            'id': [1, 2],
            'repository': [1, 1],
            'user': ['user1', 'user2'],
            'created_at': ['2023-01-01', '2023-01-02'],
            'closed_at': ['2023-01-05', '2023-01-07'],
        })

        self.sample_pr_data = pd.DataFrame({
            'id': [1, 2],
            'repository': [1, 1],
            'user': ['user3', 'user4'],
            'created_at': ['2023-01-01', '2023-01-02'],
            'merged_at': ['2023-01-06', '2023-01-08'],
        })

    def test_calculate_issue_resolution_time(self):
        result = calculate_issue_resolution_time(self.sample_issues_data)
        self.assertIn('resolution_time_days', result.columns)
        self.assertTrue(all(result['resolution_time_days'] > 0))

    def test_calculate_pr_merge_time(self):
        result = calculate_pr_merge_time(self.sample_pr_data)
        self.assertIn('merge_time_days', result.columns)
        self.assertTrue(all(result['merge_time_days'] > 0))

    def test_aggregate_repository_metrics(self):
        issues_df = calculate_issue_resolution_time(self.sample_issues_data.copy())
        pr_df = calculate_pr_merge_time(self.sample_pr_data.copy())

        # Verify that the resolution_time_days column exists
        self.assertIn('resolution_time_days', issues_df.columns)

        result = aggregate_repository_metrics(self.sample_repo_data.copy(), issues_df, pr_df)

        self.assertIn('avg_issue_resolution_days', result.columns)
        self.assertIn('avg_pr_merge_time_days', result.columns)

    def test_calculate_contributor_activity(self):
        result = calculate_contributor_activity(self.sample_issues_data, self.sample_pr_data)
        self.assertIn('total_contributors', result.columns)
        self.assertEqual(result['total_contributors'].iloc[0], 4)

    @patch('data_processing.transformer.calculate_issue_resolution_time')
    @patch('data_processing.transformer.calculate_pr_merge_time')
    @patch('data_processing.transformer.aggregate_repository_metrics')
    @patch('data_processing.transformer.calculate_contributor_activity')
    def test_transform_data(self, mock_calc_contributors, mock_agg_metrics, mock_pr_time, mock_issue_time):
        mock_issue_time.return_value = self.sample_issues_data
        mock_pr_time.return_value = self.sample_pr_data
        mock_agg_metrics.return_value = self.sample_repo_data
        mock_calc_contributors.return_value = pd.DataFrame({'repository': [1], 'total_contributors': [4]})

        repo_df, issues_df, pr_df = transform_all_data(self.sample_repo_data, self.sample_issues_data, self.sample_pr_data)

        self.assertIsInstance(repo_df, pd.DataFrame)
        self.assertIsInstance(issues_df, pd.DataFrame)
        self.assertIsInstance(pr_df, pd.DataFrame)

        mock_issue_time.assert_called_once()
        mock_pr_time.assert_called_once()
        mock_agg_metrics.assert_called_once()
        mock_calc_contributors.assert_called_once()

if __name__ == '__main__':
    unittest.main()