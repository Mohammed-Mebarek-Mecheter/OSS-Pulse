import streamlit as st
import pandas as pd

def generate_executive_summary(repo_data: pd.DataFrame, issues_data: pd.DataFrame, pr_data: pd.DataFrame):
    """
    Generates a concise executive summary of the key metrics and insights.
    """
    total_repos = len(repo_data)
    total_stars = repo_data['stars'].sum()
    total_forks = repo_data['forks'].sum()
    total_issues = len(issues_data)
    total_prs = len(pr_data)

    avg_issue_resolution = issues_data[issues_data['resolution_time_days'] >= 0]['resolution_time_days'].mean()
    avg_pr_merge_time = pr_data[pr_data['merge_time_days'] >= 0]['merge_time_days'].mean()

    summary = f"""
    📊 **OSS Pulse Summary**

    Analyzing {total_repos:,} repositories with a total of {total_stars:,} stars and {total_forks:,} forks.

    🔍 **Key Insights:**
    - {total_issues:,} issues tracked, with an average resolution time of {avg_issue_resolution:.2f} days.
    - {total_prs:,} pull requests processed, merging on average in {avg_pr_merge_time:.2f} days.
    - Top repository: {repo_data.iloc[0]['name']} with {repo_data.iloc[0]['stars']:,} stars.

    💡 **Trend Alert:** 
    {generate_trend_insight(repo_data, issues_data, pr_data)}
    """

    st.markdown(summary)

def generate_trend_insight(repo_data: pd.DataFrame, issues_data: pd.DataFrame, pr_data: pd.DataFrame) -> str:
    """
    Generates a key trend insight based on the data.
    """
    now = pd.Timestamp.now(tz='UTC')
    recent_repos = len(repo_data[repo_data['created_at'] > (now - pd.Timedelta(days=30))])
    if recent_repos > len(repo_data) * 0.1:  # If more than 10% of repos are new
        return f"Significant growth observed with {recent_repos} new repositories in the last 30 days."
    else:
        return "Repository creation rate remains stable."
