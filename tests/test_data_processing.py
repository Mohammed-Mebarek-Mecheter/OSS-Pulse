import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# Import the functions from cleaner.py and transformer.py
from data_processing.cleaner import (
    clean_repository_data,
    clean_issues_data,
    clean_pull_requests_data,
    remove_duplicates,
    handle_outliers,
    clean_all_data,
)

from data_processing.transformer import (
    calculate_repo_metrics,
    calculate_issue_metrics,
    calculate_pr_metrics,
    categorize_repositories,
    aggregate_repo_data,
    transform_all_data,
    update_pocketbase_data
)

# Sample data for testing
REPO_DATA = pd.DataFrame({
    'id': ['1', '2'],
    'name': ['Repo1', 'Repo2'],
    'full_name': ['user/Repo1', 'user/Repo2'],
    'description': ['Description 1', None],
    'stars': [10, 20],
    'forks': [2, 5],
    'open_issues': [1, 3],
    'created_at': ['2022-01-01T00:00:00Z', '2023-01-01T00:00:00Z'],
    'updated_at': ['2022-01-15T00:00:00Z', '2023-01-15T00:00:00Z'],
})

ISSUE_DATA = pd.DataFrame({
    'id': ['1', '2'],
    'number': [101, 102],
    'title': ['Issue 1', 'Issue 2'],
    'state': ['open', 'closed'],
    'created_at': ['2022-01-02T00:00:00Z', '2022-01-03T00:00:00Z'],
    'updated_at': ['2022-01-10T00:00:00Z', '2022-01-11T00:00:00Z'],
    'closed_at': [None, '2022-01-12T00:00:00Z'],
    'repository': ['1', '1'],
})

PR_DATA = pd.DataFrame({
    'id': ['1', '2'],
    'number': [201, 202],
    'title': ['PR 1', 'PR 2'],
    'state': ['open', 'merged'],
    'created_at': ['2022-01-04T00:00:00Z', '2022-01-05T00:00:00Z'],
    'updated_at': ['2022-01-14T00:00:00Z', '2022-01-15T00:00:00Z'],
    'closed_at': [None, '2022-01-16T00:00:00Z'],
    'merged_at': [None, '2022-01-17T00:00:00Z'],
    'repository': ['1', '1'],
})


# Test cleaning functions in cleaner.py

def test_clean_repository_data():
    repo_df_clean = clean_repository_data(REPO_DATA.copy())
    assert pd.api.types.is_datetime64_any_dtype(repo_df_clean['created_at'])
    assert pd.api.types.is_numeric_dtype(repo_df_clean['stars'])
    assert repo_df_clean['description'].isna().sum() == 0


def test_clean_issues_data():
    issues_df_clean = clean_issues_data(ISSUE_DATA.copy())
    assert pd.api.types.is_datetime64_any_dtype(issues_df_clean['created_at'])
    assert pd.api.types.is_numeric_dtype(issues_df_clean['number'])
    assert all(issues_df_clean['state'].isin(['open', 'closed', 'unknown']))


def test_clean_pull_requests_data():
    pr_df_clean = clean_pull_requests_data(PR_DATA.copy())
    assert pd.api.types.is_datetime64_any_dtype(pr_df_clean['created_at'])
    assert pd.api.types.is_numeric_dtype(pr_df_clean['number'])
    assert all(pr_df_clean['state'].isin(['open', 'closed', 'merged', 'unknown']))


def test_remove_duplicates():
    df_with_duplicates = pd.concat([REPO_DATA, REPO_DATA.iloc[[0]]], ignore_index=True)
    df_no_duplicates = remove_duplicates(df_with_duplicates, subset=['full_name'])
    assert len(df_no_duplicates) == len(REPO_DATA)


def test_handle_outliers():
    repo_df_outliers = REPO_DATA.copy()
    repo_df_outliers.loc[0, 'stars'] = 100000  # Introduce an extreme outlier
    repo_df_no_outliers = handle_outliers(repo_df_outliers, 'stars', method='percentile', threshold=0.99)

    # Check that the outlier has been clipped correctly
    assert repo_df_no_outliers['stars'].max() < 100000

# Test transformation functions in transformer.py

def test_calculate_repo_metrics():
    repo_df_metrics = calculate_repo_metrics(REPO_DATA.copy())
    assert 'age_days' in repo_df_metrics.columns
    assert 'stars_per_day' in repo_df_metrics.columns
    assert 'forks_per_star' in repo_df_metrics.columns
    assert repo_df_metrics['stars_per_day'].iloc[0] > 0


def calculate_issue_metrics(issues_df):
    issues_df['age_days'] = (datetime.now(issues_df['created_at'].dt.tz).replace(tzinfo=None) - issues_df[
        'created_at'].dt.tz_localize(None)).dt.days
    # Calculate time to close for closed issues
    closed_issues = issues_df[issues_df['state'] == 'closed'].copy()
    closed_issues['time_to_close_days'] = (closed_issues['closed_at'] - closed_issues[
        'created_at']).dt.total_seconds() / (24 * 60 * 60)
    issues_df = issues_df.merge(closed_issues[['id', 'time_to_close_days']], on='id', how='left')
    return issues_df

def test_calculate_pr_metrics():
    pr_df_metrics = calculate_pr_metrics(PR_DATA.copy())
    assert 'age_days' in pr_df_metrics.columns
    assert 'time_to_close_days' in pr_df_metrics.columns
    assert 'is_merged' in pr_df_metrics.columns
    merged_pr = pr_df_metrics.loc[pr_df_metrics['state'] == 'merged'].iloc[0]
    assert merged_pr['time_to_close_days'] > 0


def test_categorize_repositories():
    repo_df_categorized = categorize_repositories(REPO_DATA.copy())
    assert 'size_category' in repo_df_categorized.columns
    assert 'activity_category' in repo_df_categorized.columns
    assert repo_df_categorized['size_category'].iloc[0] in ['small', 'medium', 'large', 'very_large']


def test_aggregate_repo_data():
    repo_df_agg = aggregate_repo_data(REPO_DATA.copy(), ISSUE_DATA.copy(), PR_DATA.copy())
    assert 'total_issues' in repo_df_agg.columns
    assert 'merge_rate' in repo_df_agg.columns
    assert repo_df_agg['total_issues'].iloc[0] > 0


# Test full data processing

@patch('data_processing.cleaner.authenticate_pocketbase')
@patch('data_processing.cleaner.fetch_data_from_pocketbase')
def test_clean_all_data(mock_fetch_data, mock_authenticate):
    mock_authenticate.return_value = None
    mock_fetch_data.side_effect = [REPO_DATA, ISSUE_DATA, PR_DATA]

    repo_clean, issues_clean, pr_clean = clean_all_data()

    assert not repo_clean.empty
    assert not issues_clean.empty
    assert not pr_clean.empty


@patch('data_processing.transformer.authenticate_pocketbase')
@patch('data_processing.transformer.update_pocketbase_data')
def test_transform_all_data(mock_update_data, mock_authenticate):
    mock_authenticate.return_value = None
    mock_update_data.return_value = None

    # Ensure mock data is correctly returned
    with patch('data_processing.cleaner.fetch_data_from_pocketbase', side_effect=[REPO_DATA, ISSUE_DATA, PR_DATA]):
        repo_df_clean, issues_df_clean, pr_df_clean = clean_all_data()
        repo_transformed, issues_transformed, pr_transformed = transform_all_data(repo_df_clean, issues_df_clean,
                                                                                  pr_df_clean)

    assert 'age_days' in repo_transformed.columns
    assert 'size_category' in repo_transformed.columns
    assert 'total_issues' in repo_transformed.columns

    # Ensure the update function is called correctly
    mock_update_data.assert_any_call(repo_transformed, 'repositories')
    mock_update_data.assert_any_call(issues_transformed, 'issues')
    mock_update_data.assert_any_call(pr_transformed, 'pull_requests')


if __name__ == "__main__":
    pytest.main()
