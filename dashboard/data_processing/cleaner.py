# dashboard/data_processing/cleaner.py

import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_to_datetime(df, columns):
    """Convert specified columns to datetime with UTC timezone."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
    return df

def clean_repository_data(repo_df):
    """Clean the repository dataframe."""
    if repo_df.empty:
        logging.warning("Repository DataFrame is empty")
        return repo_df

    # Remove unnecessary columns
    columns_to_drop = ['Unnamed: 0', 'repository_y']
    repo_df = repo_df.drop(columns=columns_to_drop, errors='ignore')

    # Fill NaN in description and other text fields before type conversion
    repo_df['description'] = repo_df['description'].fillna('')

    # Convert date columns to datetime
    repo_df = convert_to_datetime(repo_df, ['created_at', 'updated_at'])

    # Handle numeric columns
    numeric_columns = ['stars', 'forks', 'open_issues']
    repo_df[numeric_columns] = repo_df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)

    return repo_df

def clean_issues_data(issues_df):
    """Clean the issues dataframe."""
    if issues_df.empty:
        logging.warning("Issues DataFrame is empty")
        return issues_df

    # Remove unnecessary columns
    columns_to_drop = ['Unnamed: 0', 'expand', 'collection_id', 'collection_name']
    issues_df = issues_df.drop(columns=columns_to_drop, errors='ignore')

    # Convert date columns to datetime
    issues_df = convert_to_datetime(issues_df, ['created_at', 'updated_at', 'closed_at'])

    return issues_df

def clean_pull_requests_data(pr_df):
    """Clean the pull requests dataframe."""
    if pr_df.empty:
        logging.warning("Pull Requests DataFrame is empty")
        return pr_df

    # Remove unnecessary columns
    columns_to_drop = ['Unnamed: 0', 'expand', 'collection_id', 'collection_name']
    pr_df = pr_df.drop(columns=columns_to_drop, errors='ignore')

    # Convert date columns to datetime
    pr_df = convert_to_datetime(pr_df, ['created_at', 'updated_at', 'closed_at', 'merged_at'])

    return pr_df

def clean_all_data(repo_df, issues_df, pr_df):
    """Main function to clean all datasets."""
    repo_df_clean = clean_repository_data(repo_df)
    issues_df_clean = clean_issues_data(issues_df)
    pr_df_clean = clean_pull_requests_data(pr_df)

    # Drop duplicates
    repo_df_clean = repo_df_clean.drop_duplicates(subset=['full_name'], keep='last')
    issues_df_clean = issues_df_clean.drop_duplicates(subset=['number', 'repository'], keep='last')
    pr_df_clean = pr_df_clean.drop_duplicates(subset=['number', 'repository'], keep='last')

    return repo_df_clean, issues_df_clean, pr_df_clean

