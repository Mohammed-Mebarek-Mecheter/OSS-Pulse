import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from data_processing.cleaner import (
    clean_repository_data,
    clean_issues_data,
    clean_pull_requests_data,
    remove_duplicates,
    handle_outliers,
    clean_all_data
)
from data_processing.transformer import (
    calculate_repo_metrics,
    calculate_issue_metrics,
    calculate_pr_metrics,
    categorize_repositories,
    aggregate_repo_data,
    transform_all_data
)


# Sample data for testing
@pytest.fixture
def sample_repo_data():
    return pd.DataFrame({
        'id': ['1', '2', '3'],
        'name': ['repo1', 'repo2', 'repo3'],
        'full_name': ['owner/repo1', 'owner/repo2', 'owner/repo3'],
        'description': ['desc1', np.nan, 'desc3'],
        'stars': [100, 200, np.nan],
        'forks': [50, np.nan, 150],
        'open_issues': [10, 20, 30],
        'created_at': ['2022-01-01', '2022-02-01', '2022-03-01'],
        'updated_at': ['2023-01-01', '2023-02-01', '2023-03-01']
    })


@pytest.fixture
def sample_issues_data():
    return pd.DataFrame({
        'id': ['1', '2', '3'],
        'number': [1, 2, 3],
        'title': ['Issue 1', 'Issue 2', 'Issue 3'],
        'state': ['open', 'closed', 'invalid'],
        'created_at': ['2022-01-01', '2022-02-01', '2022-03-01'],
        'updated_at': ['2023-01-01', '2023-02-01', '2023-03-01'],
        'closed_at': [np.nan, '2023-02-15', np.nan],
        'repository': ['1', '1', '2']
    })


@pytest.fixture
def sample_pr_data():
    return pd.DataFrame({
        'id': ['1', '2', '3'],
        'number': [1, 2, 3],
        'title': ['PR 1', 'PR 2', 'PR 3'],
        'state': ['open', 'closed', 'merged'],
        'created_at': ['2022-01-01', '2022-02-01', '2022-03-01'],
        'updated_at': ['2023-01-01', '2023-02-01', '2023-03-01'],
        'closed_at': [np.nan, '2023-02-15', '2023-03-15'],
        'merged_at': [np.nan, np.nan, '2023-03-15'],
        'repository': ['1', '1', '2']
    })


# Tests for cleaner.py

def test_clean_repository_data(sample_repo_data):
    cleaned_data = clean_repository_data(sample_repo_data)
    assert cleaned_data['stars'].dtype == 'float64'
    assert cleaned_data['forks'].dtype == 'float64'
    assert cleaned_data['open_issues'].dtype == 'float64'
    assert cleaned_data['description'].dtype == 'object'
    assert cleaned_data['description'].isnull().sum() == 0
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['created_at'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['updated_at'])


def test_clean_issues_data(sample_issues_data):
    cleaned_data = clean_issues_data(sample_issues_data)
    assert cleaned_data['number'].dtype == 'Int64'
    assert cleaned_data['state'].isin(['open', 'closed', 'unknown']).all()
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['created_at'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['updated_at'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['closed_at'])


def test_clean_pull_requests_data(sample_pr_data):
    cleaned_data = clean_pull_requests_data(sample_pr_data)
    assert cleaned_data['number'].dtype == 'Int64'
    assert cleaned_data['state'].isin(['open', 'closed', 'merged', 'unknown']).all()
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['created_at'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['updated_at'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['closed_at'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['merged_at'])


def test_remove_duplicates():
    df = pd.DataFrame({
        'id': [1, 2, 3, 3],
        'value': ['a', 'b', 'c', 'd']
    })
    result = remove_duplicates(df, subset=['id'])
    assert len(result) == 3
    assert result['value'].iloc[-1] == 'd'


def test_handle_outliers():
    df = pd.DataFrame({
        'value': [1, 2, 3, 4, 100]
    })
    result = handle_outliers(df, 'value', method='iqr')
    assert result['value'].max() < 100


@patch('cleaner.fetch_data_from_pocketbase')
def test_clean_all_data(mock_fetch):
    mock_fetch.side_effect = [
        pd.DataFrame({'id': ['1'], 'name': ['repo1']}),
        pd.DataFrame({'id': ['1'], 'number': [1], 'state': ['open']}),
        pd.DataFrame({'id': ['1'], 'number': [1], 'state': ['open']})
    ]
    repo_clean, issues_clean, pr_clean = clean_all_data()
    assert isinstance(repo_clean, pd.DataFrame)
    assert isinstance(issues_clean, pd.DataFrame)
    assert isinstance(pr_clean, pd.DataFrame)


# Tests for transformer.py

def test_calculate_repo_metrics(sample_repo_data):
    result = calculate_repo_metrics(sample_repo_data)
    assert 'age_days' in result.columns
    assert 'stars_per_day' in result.columns
    assert 'forks_per_star' in result.columns
    assert 'issue_to_star_ratio' in result.columns


def test_calculate_issue_metrics(sample_issues_data):
    result = calculate_issue_metrics(sample_issues_data)
    assert 'age_days' in result.columns
    assert 'time_to_close_days' in result.columns


def test_calculate_pr_metrics(sample_pr_data):
    result = calculate_pr_metrics(sample_pr_data)
    assert 'age_days' in result.columns
    assert 'time_to_close_days' in result.columns
    assert 'is_merged' in result.columns


def test_categorize_repositories(sample_repo_data):
    result = categorize_repositories(sample_repo_data)
    assert 'size_category' in result.columns
    assert 'activity_category' in result.columns


def test_aggregate_repo_data(sample_repo_data, sample_issues_data, sample_pr_data):
    result = aggregate_repo_data(sample_repo_data, sample_issues_data, sample_pr_data)
    assert 'total_issues' in result.columns
    assert 'open_issue_rate' in result.columns
    assert 'avg_time_to_close_issue' in result.columns
    assert 'total_prs' in result.columns
    assert 'merge_rate' in result.columns
    assert 'avg_time_to_close_pr' in result.columns


@patch('transformer.clean_all_data')
def test_transform_all_data(mock_clean_all_data, sample_repo_data, sample_issues_data, sample_pr_data):
    mock_clean_all_data.return_value = (sample_repo_data, sample_issues_data, sample_pr_data)
    repo_transformed, issues_transformed, pr_transformed = transform_all_data(sample_repo_data, sample_issues_data,
                                                                              sample_pr_data)
    assert isinstance(repo_transformed, pd.DataFrame)
    assert isinstance(issues_transformed, pd.DataFrame)
    assert isinstance(pr_transformed, pd.DataFrame)
    assert 'age_days' in repo_transformed.columns
    assert 'age_days' in issues_transformed.columns
    assert 'age_days' in pr_transformed.columns


# Integration test
@patch('transformer.clean_all_data')
@patch('transformer.update_pocketbase_data')
def test_data_processing_pipeline(mock_update_pocketbase, mock_clean_all_data, sample_repo_data, sample_issues_data,
                                  sample_pr_data):
    mock_clean_all_data.return_value = (sample_repo_data, sample_issues_data, sample_pr_data)

    from data_processing.transformer import transform_all_data
    repo_transformed, issues_transformed, pr_transformed = transform_all_data(sample_repo_data, sample_issues_data,
                                                                              sample_pr_data)

    assert isinstance(repo_transformed, pd.DataFrame)
    assert isinstance(issues_transformed, pd.DataFrame)
    assert isinstance(pr_transformed, pd.DataFrame)

    assert 'age_days' in repo_transformed.columns
    assert 'size_category' in repo_transformed.columns
    assert 'activity_category' in repo_transformed.columns

    assert 'age_days' in issues_transformed.columns
    assert 'time_to_close_days' in issues_transformed.columns

    assert 'age_days' in pr_transformed.columns
    assert 'is_merged' in pr_transformed.columns

    assert mock_update_pocketbase.call_count == 3  # Called for repos, issues, and PRs