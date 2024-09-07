# metrics_display.py
import streamlit as st
import pandas as pd
import plotly.express as px

@st.cache_data
def display_key_metrics(repo_data: pd.DataFrame, issues_data: pd.DataFrame, pr_data: pd.DataFrame):
    """
    Displays key metrics for repositories, issues, and pull requests.
    """
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Repositories", f"{len(repo_data):,}")
        st.metric("Total Stars", f"{repo_data['stars'].sum():,}")

    with col2:
        st.metric("Open Issues", f"{repo_data['open_issues'].sum():,}")
        st.metric("Total Forks", f"{repo_data['forks'].sum():,}")

    with col3:
        st.metric("Active PRs", f"{len(pr_data[pr_data['state'] == 'open']):,}")
        st.metric("Avg. Contributors", f"{repo_data['total_contributors'].mean():.2f}")

def display_advanced_metrics(repo_data: pd.DataFrame, issues_data: pd.DataFrame, pr_data: pd.DataFrame):
    """
    Displays advanced metrics and insights.
    """
    col1, col2 = st.columns(2)

    with col1:
        avg_resolution_time = issues_data[issues_data['resolution_time_days'] >= 0]['resolution_time_days'].mean()
        st.metric("Avg. Issue Resolution (days)", f"{avg_resolution_time:.2f}")

        stars_per_fork = repo_data['stars'].sum() / repo_data['forks'].sum() if repo_data['forks'].sum() > 0 else 0
        st.metric("Stars per Fork", f"{stars_per_fork:.2f}")

    with col2:
        avg_merge_time = pr_data[pr_data['merge_time_days'] >= 0]['merge_time_days'].mean()
        st.metric("Avg. PR Merge Time (days)", f"{avg_merge_time:.2f}")

        issues_per_repo = len(issues_data) / len(repo_data) if len(repo_data) > 0 else 0
        st.metric("Issues per Repository", f"{issues_per_repo:.2f}")

    # Activity heatmap
    activity_data = repo_data.copy()
    activity_data['Year'] = activity_data['created_at'].dt.year
    activity_data['Month'] = activity_data['created_at'].dt.month
    activity_data = activity_data.groupby(['Year', 'Month']).size().reset_index(name='Count')
    fig = px.density_heatmap(activity_data, x='Month', y='Year', z='Count', title='Repository Activity Heatmap', color_continuous_scale='viridis')
    return fig
