# cleaner.py
import logging
import pandas as pd
import numpy as np
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def authenticate_pocketbase():
    try:
        pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        logging.info("Successfully authenticated with PocketBase")
    except ClientResponseError as e:
        logging.error(f"Failed to authenticate with PocketBase: {e}")
        raise

def fetch_data_from_pocketbase(collection_name):
    """
    Fetch data from a PocketBase collection and return as a DataFrame.
    """
    try:
        records = pb.collection(collection_name).get_full_list()
        df = pd.DataFrame([record.dict() for record in records])
        return df
    except ClientResponseError as e:
        logging.error(f"Error fetching data from {collection_name}: {e}")
        return pd.DataFrame()

def convert_to_datetime(df, columns):
    """
    Convert specified columns to datetime with UTC timezone.
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
    return df

def clean_repository_data(repo_df):
    """
    Clean the repository dataframe.
    """
    if repo_df.empty:
        logging.warning("Repository DataFrame is empty")
        return repo_df

    # Convert date columns to datetime
    date_columns = ['created_at', 'updated_at']
    repo_df = convert_to_datetime(repo_df, date_columns)

    # Ensure numeric columns are of the correct type
    numeric_columns = ['stars', 'forks', 'open_issues']
    repo_df[numeric_columns] = repo_df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # Fill NaN values in numeric columns with 0
    repo_df[numeric_columns] = repo_df[numeric_columns].fillna(0)

    # Ensure 'description' is a string and replace NaN with empty string
    repo_df['description'] = repo_df['description'].fillna('').astype(str)

    return repo_df

def clean_issues_data(issues_df):
    """
    Clean the issues dataframe.
    """
    if issues_df.empty:
        logging.warning("Issues DataFrame is empty")
        return issues_df

    # Convert date columns to datetime
    date_columns = ['created_at', 'updated_at', 'closed_at']
    issues_df = convert_to_datetime(issues_df, date_columns)

    # Ensure 'number' is integer
    issues_df['number'] = pd.to_numeric(issues_df['number'], errors='coerce').astype('Int64')

    # Ensure 'state' is a valid value
    issues_df['state'] = issues_df['state'].apply(lambda x: x if x in ['open', 'closed'] else 'unknown')

    # Ensure 'title' is a string and replace NaN with empty string
    issues_df['title'] = issues_df['title'].fillna('').astype(str)

    return issues_df

def clean_pull_requests_data(pr_df):
    """
    Clean the pull requests dataframe.
    """
    if pr_df.empty:
        logging.warning("Pull Requests DataFrame is empty")
        return pr_df

    # Convert date columns to datetime
    date_columns = ['created_at', 'updated_at', 'closed_at', 'merged_at']
    pr_df = convert_to_datetime(pr_df, date_columns)

    # Ensure 'number' is integer
    pr_df['number'] = pd.to_numeric(pr_df['number'], errors='coerce').astype('Int64')

    # Ensure 'state' is a valid value
    pr_df['state'] = pr_df['state'].apply(lambda x: x if x in ['open', 'closed', 'merged'] else 'unknown')

    # Ensure 'title' is a string and replace NaN with empty string
    pr_df['title'] = pr_df['title'].fillna('').astype(str)

    return pr_df

def remove_duplicates(df, subset):
    """
    Remove duplicate rows based on specified columns.
    """
    return df.drop_duplicates(subset=subset, keep='last')


def handle_outliers(df, column, method='percentile', threshold=0.99):
    """
    Handle outliers in the specified column.
    """
    if method == 'percentile':
        upper_bound = df[column].quantile(threshold)  # Top 1% as outliers
        df[column] = df[column].clip(upper=upper_bound)
    elif method == 'log_transform':
        df[column] = np.log1p(df[column])  # Logarithmic transformation
    elif method == 'iqr':
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df[column] = df[column].clip(lower=lower_bound, upper=upper_bound)
    elif method == 'zscore':
        z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
        df[column] = df[column].mask(z_scores > threshold, df[column].median())
    return df

def clean_all_data():
    """
    Main function to clean all datasets.
    """
    authenticate_pocketbase()

    # Fetch data from PocketBase
    repo_df = fetch_data_from_pocketbase('repositories')
    issues_df = fetch_data_from_pocketbase('issues')
    pr_df = fetch_data_from_pocketbase('pull_requests')

    # Clean each dataset
    repo_df_clean = clean_repository_data(repo_df)
    issues_df_clean = clean_issues_data(issues_df)
    pr_df_clean = clean_pull_requests_data(pr_df)

    # Remove duplicates
    repo_df_clean = remove_duplicates(repo_df_clean, subset=['full_name'])
    issues_df_clean = remove_duplicates(issues_df_clean, subset=['number', 'repository'])
    pr_df_clean = remove_duplicates(pr_df_clean, subset=['number', 'repository'])

    # Handle outliers in numeric columns
    for column in ['stars', 'forks', 'open_issues']:
        repo_df_clean = handle_outliers(repo_df_clean, column)

    return repo_df_clean, issues_df_clean, pr_df_clean

if __name__ == "__main__":
    repo_clean, issues_clean, pr_clean = clean_all_data()
    print("Data cleaning completed.")
