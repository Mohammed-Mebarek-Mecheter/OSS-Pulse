# data_loader.py

import pandas as pd
import os
from typing import Tuple, Optional

def load_data() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Load repository, issues, and pull request data from parquet files.

    Returns:
    - Tuple of DataFrames (repo_data, issues_data, pr_data)
    - Returns None for any DataFrame if the corresponding file is not found
    """
    base_path = os.path.join('dashboard', 'data_processing', 'data')

    try:
        repo_data = pd.read_parquet(os.path.join(base_path, 'repo_data.parquet'))
    except FileNotFoundError:
        print("Repository data file not found.")
        repo_data = None

    try:
        issues_data = pd.read_parquet(os.path.join(base_path, 'issues_data.parquet'))
    except FileNotFoundError:
        print("Issues data file not found.")
        issues_data = None

    try:
        pr_data = pd.read_parquet(os.path.join(base_path, 'pr_data.parquet'))
    except FileNotFoundError:
        print("Pull request data file not found.")
        pr_data = None

    return repo_data, issues_data, pr_data