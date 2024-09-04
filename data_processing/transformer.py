# transformer.py
import logging

import pandas as pd
from pocketbase import PocketBase
from pocketbase.client import ClientResponseError
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PocketBase configuration
POCKETBASE_URL = os.getenv("POCKETBASE_URL")
POCKETBASE_EMAIL = os.getenv("POCKETBASE_EMAIL")
POCKETBASE_PASSWORD = os.getenv("POCKETBASE_PASSWORD")

# Initialize PocketBase client
pb = PocketBase(POCKETBASE_URL)


def authenticate_pocketbase():
    try:
        pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        print("Successfully authenticated with PocketBase")
    except ClientResponseError as e:
        print(f"Failed to authenticate with PocketBase: {e}")
        raise


def calculate_issue_resolution_time(issues_df):
    """Calculate issue resolution time in days."""
    issues_df = issues_df.copy()  # Create a copy to avoid SettingWithCopyWarning
    issues_df['created_at'] = pd.to_datetime(issues_df['created_at'], utc=True)
    issues_df['closed_at'] = pd.to_datetime(issues_df['closed_at'], utc=True)
    issues_df['resolution_time_days'] = (issues_df['closed_at'] - issues_df['created_at']).dt.total_seconds() / (24 * 3600)
    issues_df['resolution_time_days'] = issues_df['resolution_time_days'].fillna(-1)  # Unresolved issues set to -1
    return issues_df


def calculate_pr_merge_time(pr_df):
    """Calculate pull request merge time in days."""
    pr_df = pr_df.copy()  # Create a copy to avoid SettingWithCopyWarning
    pr_df['created_at'] = pd.to_datetime(pr_df['created_at'], utc=True)
    pr_df['merged_at'] = pd.to_datetime(pr_df['merged_at'], utc=True)
    pr_df['merge_time_days'] = (pr_df['merged_at'] - pr_df['created_at']).dt.total_seconds() / (24 * 3600)
    pr_df['merge_time_days'] = pr_df['merge_time_days'].fillna(-1)  # Unmerged PRs set to -1
    return pr_df


def aggregate_repository_metrics(repo_df, issues_df, pr_df):
    """Aggregate metrics for each repository."""
    if issues_df.empty:
        logging.warning("Issues data is empty, skipping issue resolution aggregation.")
        avg_issue_resolution = pd.DataFrame(columns=['id', 'avg_issue_resolution_days'])
    else:
        avg_issue_resolution = issues_df.groupby('repository')['resolution_time_days'].mean().reset_index()
        avg_issue_resolution.columns = ['id', 'avg_issue_resolution_days']

    if pr_df.empty:
        logging.warning("Pull Requests data is empty, skipping PR merge time aggregation.")
        avg_pr_merge_time = pd.DataFrame(columns=['id', 'avg_pr_merge_time_days'])
    else:
        avg_pr_merge_time = pr_df.groupby('repository')['merge_time_days'].mean().reset_index()
        avg_pr_merge_time.columns = ['id', 'avg_pr_merge_time_days']

    # Merge metrics with repository dataframe
    result_df = repo_df.merge(avg_issue_resolution, on='id', how='left')
    result_df = result_df.merge(avg_pr_merge_time, on='id', how='left')

    # Fill missing values with -1 to indicate lack of data
    result_df = result_df.fillna({
        'avg_issue_resolution_days': -1,
        'avg_pr_merge_time_days': -1
    })

    return result_df


def calculate_contributor_activity(issues_df, pr_df):
    """Calculate the number of contributors per repository."""
    issues_contributors = issues_df.groupby('repository')['user'].nunique().reset_index()
    issues_contributors.columns = ['repository', 'issue_contributors']

    pr_contributors = pr_df.groupby('repository')['user'].nunique().reset_index()
    pr_contributors.columns = ['repository', 'pr_contributors']

    contributors_df = issues_contributors.merge(pr_contributors, on='repository', how='outer')
    contributors_df.fillna(0, inplace=True)
    contributors_df['total_contributors'] = contributors_df['issue_contributors'] + contributors_df['pr_contributors']

    return contributors_df


def categorize_repositories(repo_df):
    """Categorize repositories based on size and activity."""
    conditions = [
        (repo_df['stars'] < 100),
        (repo_df['stars'] >= 100) & (repo_df['stars'] < 1000),
        (repo_df['stars'] >= 1000) & (repo_df['stars'] < 10000),
        (repo_df['stars'] >= 10000)
    ]
    categories = ['micro', 'small', 'medium', 'large']

    repo_df['size_category'] = pd.cut(repo_df['stars'], bins=[0, 100, 1000, 10000, float('inf')], labels=categories, right=False)

    return repo_df


def normalize_metrics(repo_df):
    """Normalize metrics to make comparisons across repositories more meaningful."""
    repo_df['stars_per_fork'] = repo_df['stars'] / repo_df['forks'].replace({0: 1})
    repo_df['stars_per_issue'] = repo_df['stars'] / repo_df['open_issues'].replace({0: 1})

    # Only calculate contributor_per_star if total_contributors column exists
    if 'total_contributors' in repo_df.columns:
        repo_df['contributor_per_star'] = repo_df['total_contributors'] / repo_df['stars'].replace({0: 1})

    return repo_df


def transform_all_data(repo_df, issues_df, pr_df):
    """Apply all transformations to the data."""
    if not issues_df.empty:
        issues_df = calculate_issue_resolution_time(issues_df)

    if not pr_df.empty:
        pr_df = calculate_pr_merge_time(pr_df)

    repo_df = aggregate_repository_metrics(repo_df, issues_df, pr_df)

    if not issues_df.empty or not pr_df.empty:
        contributors_df = calculate_contributor_activity(issues_df, pr_df)
        repo_df = repo_df.merge(contributors_df, left_on='id', right_on='repository', how='left')
        repo_df.drop(columns=['repository'], inplace=True, errors='ignore')

    # Categorize repositories
    repo_df = categorize_repositories(repo_df)

    # Normalize metrics
    repo_df = normalize_metrics(repo_df)

    return repo_df, issues_df, pr_df

if __name__ == "__main__":
    from cleaner import clean_all_data

    authenticate_pocketbase()

    # Clean data
    repo_clean, issues_clean, pr_clean = clean_all_data()

    # Transform data
    repo_transformed, issues_transformed, pr_transformed = transform_all_data(repo_clean, issues_clean, pr_clean)

    print("Data transformation and update completed.")
