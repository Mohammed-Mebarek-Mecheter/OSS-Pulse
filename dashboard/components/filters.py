import streamlit as st
import pandas as pd

def apply_advanced_filters(repo_data, issues_data, pr_data):
    """
    Provides advanced filters for stars, forks, issues, and contributors.

    Parameters:
    - repo_data: DataFrame with repository data.
    - issues_data: DataFrame with issues data.
    - pr_data: DataFrame with pull requests data.

    Returns:
    - repo_data: Filtered repository DataFrame.
    - issues_data: Filtered issues DataFrame.
    - pr_data: Filtered pull requests DataFrame.
    """
    st.sidebar.subheader("Advanced Filters")

    # Filter by stars
    min_stars, max_stars = st.sidebar.slider(
        "Filter by Stars",
        int(repo_data['stars'].min()),
        int(repo_data['stars'].max()),
        (int(repo_data['stars'].min()), int(repo_data['stars'].max()))
    )
    repo_data = repo_data[(repo_data['stars'] >= min_stars) & (repo_data['stars'] <= max_stars)]

    # Filter by forks
    min_forks, max_forks = st.sidebar.slider(
        "Filter by Forks",
        int(repo_data['forks'].min()),
        int(repo_data['forks'].max()),
        (int(repo_data['forks'].min()), int(repo_data['forks'].max()))
    )
    repo_data = repo_data[(repo_data['forks'] >= min_forks) & (repo_data['forks'] <= max_forks)]

    # Filter by open issues
    min_issues, max_issues = st.sidebar.slider(
        "Filter by Open Issues",
        int(repo_data['open_issues'].min()),
        int(repo_data['open_issues'].max()),
        (int(repo_data['open_issues'].min()), int(repo_data['open_issues'].max()))
    )
    repo_data = repo_data[(repo_data['open_issues'] >= min_issues) & (repo_data['open_issues'] <= max_issues)]

    # Filter by contributors
    min_contributors, max_contributors = st.sidebar.slider(
        "Filter by Contributors",
        int(repo_data['total_contributors'].min()),
        int(repo_data['total_contributors'].max()),
        (int(repo_data['total_contributors'].min()), int(repo_data['total_contributors'].max()))
    )
    repo_data = repo_data[(repo_data['total_contributors'] >= min_contributors) & (repo_data['total_contributors'] <= max_contributors)]

    # Filter by issue resolution time
    if not issues_data.empty:
        min_resolution_time, max_resolution_time = st.sidebar.slider(
            "Filter by Issue Resolution Time (days)",
            float(issues_data['resolution_time_days'].min()),
            float(issues_data['resolution_time_days'].max()),
            (float(issues_data['resolution_time_days'].min()), float(issues_data['resolution_time_days'].max()))
        )
        issues_data = issues_data[
            (issues_data['resolution_time_days'] >= min_resolution_time) &
            (issues_data['resolution_time_days'] <= max_resolution_time)
        ]

    # Filter by PR merge time
    if not pr_data.empty:
        min_merge_time, max_merge_time = st.sidebar.slider(
            "Filter by PR Merge Time (days)",
            float(pr_data['merge_time_days'].min()),
            float(pr_data['merge_time_days'].max()),
            (float(pr_data['merge_time_days'].min()), float(pr_data['merge_time_days'].max()))
        )
        pr_data = pr_data[
            (pr_data['merge_time_days'] >= min_merge_time) &
            (pr_data['merge_time_days'] <= max_merge_time)
        ]

    # Filter repositories by selected time period
    time_period = st.sidebar.selectbox(
        "Select Time Period",
        options=["All Time", "Last Week", "Last Month", "Last 3 Months", "Last 6 Months", "Last Year"]
    )

    if time_period != "All Time":
        end_date = pd.Timestamp.now()
        if time_period == "Last Week":
            start_date = end_date - pd.Timedelta(days=7)
        elif time_period == "Last Month":
            start_date = end_date - pd.Timedelta(days=30)
        elif time_period == "Last 3 Months":
            start_date = end_date - pd.Timedelta(days=90)
        elif time_period == "Last 6 Months":
            start_date = end_date - pd.Timedelta(days=180)
        else:  # Last Year
            start_date = end_date - pd.Timedelta(days=365)

        repo_data = repo_data[(repo_data['created_at'] >= start_date) & (repo_data['created_at'] <= end_date)]

    return repo_data, issues_data, pr_data