import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def calculate_issue_resolution_time(issues_df):
    """Calculate issue resolution time in days."""
    issues_df['created_at'] = pd.to_datetime(issues_df['created_at'], utc=True, errors='coerce')
    issues_df['closed_at'] = pd.to_datetime(issues_df['closed_at'], utc=True, errors='coerce')

    # Calculate resolution time in days
    issues_df['resolution_time_days'] = (issues_df['closed_at'] - issues_df['created_at']).dt.total_seconds() / (
                24 * 3600)

    # Fill missing resolution time with -1 for unresolved issues
    issues_df['resolution_time_days'] = issues_df['resolution_time_days'].fillna(-1)

    return issues_df

def calculate_pr_merge_time(pr_df):
    """Calculate pull request merge time in days."""
    pr_df['created_at'] = pd.to_datetime(pr_df['created_at'], utc=True, errors='coerce')
    pr_df['merged_at'] = pd.to_datetime(pr_df['merged_at'], utc=True, errors='coerce')
    pr_df['merge_time_days'] = (pr_df['merged_at'] - pr_df['created_at']).dt.total_seconds() / (24 * 3600)
    pr_df['merge_time_days'] = pr_df['merge_time_days'].fillna(-1)
    return pr_df

def categorize_repositories(repo_df):
    """Categorize repositories based on the number of stars."""
    bins = [0, 1000, 10000, 100000, 250000, float('inf')]
    labels = ['micro', 'small', 'medium', 'large', 'mega']
    repo_df['size_category'] = pd.cut(repo_df['stars'], bins=bins, labels=labels, right=False)
    return repo_df

def flag_stale_repositories(repo_df):
    """Flag repositories that haven't been updated in over 6 months as stale."""
    # Ensure 'updated_at' is in datetime format and handle invalid values
    repo_df['updated_at'] = pd.to_datetime(repo_df['updated_at'], errors='coerce', utc=True)

    # Fill any missing or invalid 'updated_at' with the current timestamp
    repo_df['updated_at'] = repo_df['updated_at'].fillna(pd.Timestamp.utcnow())

    # Calculate the stale flag using pd.Timedelta
    repo_df['stale'] = (pd.Timestamp.utcnow() - repo_df['updated_at']) > pd.Timedelta(days=180)

    return repo_df

def normalize_metrics(repo_df):
    """Normalize metrics across repositories."""
    repo_df['stars_per_fork'] = (repo_df['stars'] / repo_df['forks'].replace({0: 1})).round(2)
    repo_df['stars_per_issue'] = (repo_df['stars'] / repo_df['open_issues'].replace({0: 1})).round(2)
    repo_df['contributor_per_star'] = (repo_df['total_contributors'] / repo_df['stars'].replace({0: 1})).round(2)
    return repo_df

def calculate_contributor_activity(issues_df, pr_df):
    """Calculate the number of contributors per repository from issues and PRs."""
    # Count unique contributors in issues
    issues_contributors = issues_df.groupby('repository')['title'].nunique().reset_index()
    issues_contributors.columns = ['repository', 'issue_contributors']

    # Count unique contributors in PRs
    pr_contributors = pr_df.groupby('repository')['title'].nunique().reset_index()
    pr_contributors.columns = ['repository', 'pr_contributors']

    # Merge both datasets and calculate total contributors
    contributors_df = pd.merge(issues_contributors, pr_contributors, on='repository', how='outer').fillna(0)
    contributors_df['total_contributors'] = contributors_df['issue_contributors'] + contributors_df['pr_contributors']

    return contributors_df

def transform_all_data(repo_df, issues_df, pr_df):
    """Apply all transformations to the data."""
    # Create a mapping of repository IDs to names
    repo_id_to_name = dict(zip(repo_df['id'], repo_df['name']))

    if not issues_df.empty:
        issues_df = calculate_issue_resolution_time(issues_df)
        # Map repository IDs to names in issues_df
        issues_df['repository'] = issues_df['repository'].map(repo_id_to_name)

    if not pr_df.empty:
        pr_df = calculate_pr_merge_time(pr_df)
        # Map repository IDs to names in pr_df
        pr_df['repository'] = pr_df['repository'].map(repo_id_to_name)

    # Merge calculated contributor metrics into the repository data
    contributors_df = calculate_contributor_activity(issues_df, pr_df)
    repo_df = pd.merge(repo_df, contributors_df, left_on='name', right_on='repository', how='left').fillna(0)

    # Infer objects to avoid future warnings
    repo_df = repo_df.infer_objects(copy=False)

    # Categorize repositories and normalize metrics
    repo_df = categorize_repositories(repo_df)
    repo_df = flag_stale_repositories(repo_df)
    repo_df = normalize_metrics(repo_df)

    return repo_df, issues_df, pr_df