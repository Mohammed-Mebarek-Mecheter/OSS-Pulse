# app.py
import streamlit as st
import pandas as pd
from dashboard.components.metrics_display import display_metrics, display_advanced_metrics
from dashboard.components.sidebar import display_sidebar, apply_filters
from dashboard.components.filters import apply_advanced_filters
from dashboard.visualizations import (
    plot_repository_growth,
    plot_issue_resolution_time,
    plot_pull_request_merge_time,
    plot_contributor_activity,
    plot_repository_size_distribution,
    plot_correlation_heatmap
)
from dashboard.data_loader import load_data

# Set page config
st.set_page_config(page_title="OSS-Pulse Dashboard", page_icon="📊", layout="wide")

# Load data
@st.cache_data
def load_cached_data():
    data = load_data()
    if data is None or any(d is None for d in data):
        st.error("Failed to load data. Please check your data source and try again.")
        return None, None, None
    return data

repo_data, issues_data, pr_data = load_cached_data()

# Main app
def main():
    st.title("OSS-Pulse Dashboard")
    st.write("Analyzing Open Source Software Trends and Metrics")

    if repo_data is None or issues_data is None or pr_data is None:
        st.error("Data is not available. Please check your data source and try again.")
        return

    # Sidebar filters
    filters = display_sidebar(repo_data)
    filtered_repo_data = apply_filters(repo_data, filters)

    # Advanced filters
    filtered_repo_data, filtered_issues_data, filtered_pr_data = apply_advanced_filters(filtered_repo_data, issues_data, pr_data)

    # Display metrics
    display_metrics(filtered_repo_data, filtered_issues_data, filtered_pr_data)

    # Tabs for different visualizations
    tab1, tab2, tab3 = st.tabs(["Repository Metrics", "Issue & PR Analysis", "Advanced Metrics"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            plot_repository_growth(filtered_repo_data)
        with col2:
            plot_contributor_activity(filtered_repo_data)

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            plot_issue_resolution_time(filtered_issues_data)
        with col2:
            plot_pull_request_merge_time(filtered_pr_data)


    with tab3:
        display_advanced_metrics(filtered_repo_data, filtered_issues_data, filtered_pr_data)
        plot_correlation_heatmap(filtered_repo_data)

    # Footer
    st.markdown("---")
    st.markdown("Created with ❤️ by Mebarek")

if __name__ == "__main__":
    main()