# metrics_display.py
import streamlit as st
import pandas as pd
import plotly.express as px

def display_metrics(repo_data, issues_data, pr_data):
    """
    Displays key metrics for repositories, issues, and pull requests.

    Parameters:
    - repo_data: DataFrame with repository data.
    - issues_data: DataFrame with issues data.
    - pr_data: DataFrame with pull requests data.
    """
    st.header("Key Metrics")

    total_repos = len(repo_data)
    total_issues = len(issues_data)
    total_prs = len(pr_data)

    avg_stars = repo_data['stars'].mean()
    avg_forks = repo_data['forks'].mean()
    avg_issues = repo_data['open_issues'].mean()

    total_contributors = repo_data['total_contributors'].sum()
    avg_contributors = repo_data['total_contributors'].mean()

    # Display the metrics in columns for a neat layout
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label="Total Repositories", value=total_repos)
        st.metric(label="Average Stars", value=f"{avg_stars:.2f}")
        st.metric(label="Total Contributors", value=total_contributors)

    with col2:
        st.metric(label="Total Issues", value=total_issues)
        st.metric(label="Average Forks", value=f"{avg_forks:.2f}")
        st.metric(label="Avg Contributors per Repo", value=f"{avg_contributors:.2f}")

    with col3:
        st.metric(label="Total Pull Requests", value=total_prs)
        st.metric(label="Avg Open Issues per Repo", value=f"{avg_issues:.2f}")
        st.metric(label="Avg PRs per Repo", value=f"{len(pr_data) / total_repos:.2f}")

    # Additional insights
    st.subheader("Repository Size Distribution")
    size_distribution = repo_data['size_category'].value_counts().sort_index()
    fig = px.pie(values=size_distribution.values, names=size_distribution.index, title="Repository Size Distribution")
    st.plotly_chart(fig)

    # Top 5 repositories by stars
    st.subheader("Top 5 Repositories by Stars")
    top_repos = repo_data.nlargest(5, 'stars')[['full_name', 'stars']]
    st.table(top_repos)

def display_advanced_metrics(repo_data, issues_data, pr_data):
    """
    Displays advanced metrics and insights.
    """
    st.header("Advanced Metrics")

    # Calculate and display average resolution time for issues
    avg_resolution_time = issues_data[issues_data['resolution_time_days'] >= 0]['resolution_time_days'].mean()
    st.metric(label="Avg Issue Resolution Time (days)", value=f"{avg_resolution_time:.2f}")

    # Calculate and display average merge time for PRs
    avg_merge_time = pr_data[pr_data['merge_time_days'] >= 0]['merge_time_days'].mean()
    st.metric(label="Avg PR Merge Time (days)", value=f"{avg_merge_time:.2f}")

    # Display stale repositories percentage
    stale_repos_pct = (repo_data['stale'].sum() / len(repo_data)) * 100
    st.metric(label="Stale Repositories (%)", value=f"{stale_repos_pct:.2f}%")

    # Correlation heatmap
    st.subheader("Correlation Heatmap")
    corr_matrix = repo_data[['stars', 'forks', 'open_issues', 'total_contributors']].corr()
    fig = px.imshow(corr_matrix, text_auto=True, aspect="auto", title="Correlation Heatmap")
    st.plotly_chart(fig)

    # Repository activity over time
    st.subheader("Repository Activity Over Time")
    repo_data['created_at'] = pd.to_datetime(repo_data['created_at'])
    repo_counts = repo_data.resample('M', on='created_at').size().reset_index(name='count')
    fig = px.line(repo_counts, x='created_at', y='count', title="New Repositories Created per Month")
    st.plotly_chart(fig)