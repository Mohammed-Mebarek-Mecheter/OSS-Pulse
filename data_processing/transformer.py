# transformer.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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


def calculate_repo_metrics(repo_df):
    """
    Calculate additional metrics for repositories.
    """
    # Calculate age of the repository in days
    repo_df['age_days'] = (
                datetime.now(repo_df['created_at'].dt.tz).replace(tzinfo=None) - repo_df['created_at'].dt.tz_localize(
            None)).dt.days

    # Calculate stars per day
    repo_df['stars_per_day'] = repo_df['stars'] / repo_df['age_days']

    # Calculate forks per star
    repo_df['forks_per_star'] = repo_df['forks'] / repo_df['stars'].replace(0, 1)  # Avoid division by zero

    # Calculate issue-to-star ratio
    repo_df['issue_to_star_ratio'] = repo_df['open_issues'] / repo_df['stars'].replace(0, 1)  # Avoid division by zero

    return repo_df


def calculate_issue_metrics(issues_df):
    """
    Calculate additional metrics for issues.
    """
    # Calculate issue age in days
    issues_df['age_days'] = (datetime.now(issues_df['created_at'].dt.tz).replace(tzinfo=None) - issues_df[
        'created_at'].dt.tz_localize(None)).dt.days

    # Calculate time to close for closed issues
    closed_issues = issues_df[issues_df['state'] == 'closed']
    closed_issues['time_to_close_days'] = (closed_issues['closed_at'] - closed_issues[
        'created_at']).dt.total_seconds() / (24 * 60 * 60)
    issues_df = issues_df.merge(closed_issues[['id', 'time_to_close_days']], on='id', how='left')

    return issues_df


def calculate_pr_metrics(pr_df):
    """
    Calculate additional metrics for pull requests.
    """
    # Calculate PR age in days
    pr_df['age_days'] = (
                datetime.now(pr_df['created_at'].dt.tz).replace(tzinfo=None) - pr_df['created_at'].dt.tz_localize(
            None)).dt.days

    # Calculate time to close/merge for closed/merged PRs
    closed_prs = pr_df[pr_df['state'].isin(['closed', 'merged'])]
    closed_prs['time_to_close_days'] = (closed_prs['closed_at'] - closed_prs['created_at']).dt.total_seconds() / (
                24 * 60 * 60)
    pr_df = pr_df.merge(closed_prs[['id', 'time_to_close_days']], on='id', how='left')

    # Calculate merge rate
    pr_df['is_merged'] = pr_df['state'] == 'merged'

    return pr_df


def categorize_repositories(repo_df):
    """
    Categorize repositories based on their metrics.
    """
    # Categorize by size (stars)
    repo_df['size_category'] = pd.cut(repo_df['stars'],
                                      bins=[0, 10, 100, 1000, np.inf],
                                      labels=['small', 'medium', 'large', 'very_large'])

    # Categorize by activity (update frequency)
    repo_df['days_since_update'] = (
                datetime.now(repo_df['updated_at'].dt.tz).replace(tzinfo=None) - repo_df['updated_at'].dt.tz_localize(
            None)).dt.days
    repo_df['activity_category'] = pd.cut(repo_df['days_since_update'],
                                          bins=[0, 7, 30, 90, np.inf],
                                          labels=['very_active', 'active', 'less_active', 'inactive'])

    return repo_df


def aggregate_repo_data(repo_df, issues_df, pr_df):
    """
    Aggregate issue and PR data into repository dataframe.
    """
    # Aggregate issue data
    issue_agg = issues_df.groupby('repository').agg({
        'id': 'count',
        'state': lambda x: (x == 'open').mean(),
        'time_to_close_days': 'mean'
    }).rename(columns={
        'id': 'total_issues',
        'state': 'open_issue_rate',
        'time_to_close_days': 'avg_time_to_close_issue'
    })

    # Aggregate PR data
    pr_agg = pr_df.groupby('repository').agg({
        'id': 'count',
        'is_merged': 'mean',
        'time_to_close_days': 'mean'
    }).rename(columns={
        'id': 'total_prs',
        'is_merged': 'merge_rate',
        'time_to_close_days': 'avg_time_to_close_pr'
    })

    # Merge aggregated data into repo_df
    repo_df = repo_df.merge(issue_agg, left_on='id', right_index=True, how='left')
    repo_df = repo_df.merge(pr_agg, left_on='id', right_index=True, how='left')

    return repo_df


def transform_all_data(repo_df, issues_df, pr_df):
    """
    Main function to transform all datasets.
    """
    # Calculate metrics for each dataset
    repo_df = calculate_repo_metrics(repo_df)
    issues_df = calculate_issue_metrics(issues_df)
    pr_df = calculate_pr_metrics(pr_df)

    # Categorize repositories
    repo_df = categorize_repositories(repo_df)

    # Aggregate data
    repo_df = aggregate_repo_data(repo_df, issues_df, pr_df)

    return repo_df, issues_df, pr_df


def update_pocketbase_data(df, collection_name):
    """
    Update PocketBase data with transformed dataframe.
    """
    for _, row in df.iterrows():
        try:
            pb.collection(collection_name).update(row['id'], row.to_dict())
        except ClientResponseError as e:
            print(f"Error updating record in {collection_name}: {e}")


if __name__ == "__main__":
    from cleaner import clean_all_data

    authenticate_pocketbase()

    # Clean data
    repo_clean, issues_clean, pr_clean = clean_all_data()

    # Transform data
    repo_transformed, issues_transformed, pr_transformed = transform_all_data(repo_clean, issues_clean, pr_clean)

    # Update PocketBase with transformed data
    update_pocketbase_data(repo_transformed, 'repositories')
    update_pocketbase_data(issues_transformed, 'issues')
    update_pocketbase_data(pr_transformed, 'pull_requests')

    print("Data transformation and update completed.")