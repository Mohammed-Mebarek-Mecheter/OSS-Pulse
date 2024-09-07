# app.py
import streamlit as st
import pandas as pd
import base64
from concurrent.futures import ThreadPoolExecutor

# Custom components
from dashboard.components.executive_summary import generate_executive_summary
from dashboard.components.sidebar import display_sidebar, apply_filters
from dashboard.components.metrics_display import display_key_metrics, display_advanced_metrics
from dashboard.data_loader import load_data
from dashboard.components.filters import apply_advanced_filters
from dashboard.visualizations import (
    plot_repository_growth,
    plot_issue_resolution_time,
    plot_pull_request_merge_time,
    plot_contributor_activity,
    plot_repository_size_distribution,
    plot_correlation_heatmap,
    plot_issue_pr_funnel,
    plot_trend_analysis,
    plot_repository_treemap,
    plot_issues_vs_prs,
    display_top_repositories
)
from streamlit_lottie import st_lottie
import json

# Set page config
st.set_page_config(page_title="OSS-Pulse", page_icon="📊", layout="wide")

# Load custom CSS
@st.cache_resource
def load_css():
    with open('dashboard/assets/style.css') as f:
        return f'<style>{f.read()}</style>'

st.markdown(load_css(), unsafe_allow_html=True)

# Data loading and caching
@st.cache_data(ttl=3600)
def get_data():
    return load_data()

# Data export function
@st.cache_data
def get_download_link(df: pd.DataFrame, filename: str, text: str) -> str:
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}" class="download-link">{text}</a>'

# Concurrent loading of visualizations
def load_visualizations(filtered_repo_data, filtered_issues_data, filtered_pr_data):
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(plot_repository_growth, filtered_repo_data),
            executor.submit(plot_issue_resolution_time, filtered_issues_data),
            executor.submit(plot_pull_request_merge_time, filtered_pr_data),
            executor.submit(plot_contributor_activity, filtered_repo_data),
            executor.submit(plot_repository_size_distribution, filtered_repo_data),
            executor.submit(plot_correlation_heatmap, filtered_repo_data),
            executor.submit(plot_issue_pr_funnel, filtered_issues_data, filtered_pr_data),
            executor.submit(plot_trend_analysis, filtered_repo_data),
            executor.submit(plot_repository_treemap, filtered_repo_data),
            executor.submit(plot_issues_vs_prs, filtered_repo_data, filtered_issues_data, filtered_pr_data)
        ]
        return [future.result() for future in futures]

@st.cache_resource
def load_lottie_files():
    lottie_files = ["github", "linkedin", "profile"]
    return {file: json.load(open(f"dashboard/assets/images/{file}.json")) for file in lottie_files}

def main():
    # Header
    st.markdown("<h1 style='text-align: center;'>OSS-Pulse</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'>Analyzing Open Source Software Trends and Metrics</p>",
                unsafe_allow_html=True)

    # Load data
    repo_data, issues_data, pr_data = get_data()

    if repo_data is None or issues_data is None or pr_data is None:
        st.error("Failed to load data. Please check your data source and try again.")
        return

    # Sidebar filters
    with st.sidebar:
        filters = display_sidebar(repo_data, issues_data, pr_data)
        filtered_repo_data, filtered_issues_data, filtered_pr_data = apply_filters(repo_data, issues_data, pr_data,
                                                                                   filters)
        if not filtered_repo_data.empty:
            filtered_repo_data, filtered_issues_data, filtered_pr_data = apply_advanced_filters(filtered_repo_data,
                                                                                                filtered_issues_data,
                                                                                                filtered_pr_data)
        else:
            st.warning("No data available after applying filters. Please adjust your filter criteria.")

    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Repositories", "Issues & PRs", "Insights"])

    with tab1:
        display_key_metrics(filtered_repo_data, filtered_issues_data, filtered_pr_data)
        generate_executive_summary(filtered_repo_data, filtered_issues_data, filtered_pr_data)

    # Load visualizations concurrently
    visualizations = load_visualizations(filtered_repo_data, filtered_issues_data, filtered_pr_data)

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(visualizations[0], use_container_width=True)
        with col2:
            st.plotly_chart(visualizations[3], use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(visualizations[4], use_container_width=True)
        with col2:
            display_top_repositories(filtered_repo_data)

    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(visualizations[1], use_container_width=True)
        with col2:
            st.plotly_chart(visualizations[2], use_container_width=True)

        st.plotly_chart(visualizations[6], use_container_width=True)
        st.plotly_chart(visualizations[9], use_container_width=True)

    with tab4:
        advanced_metrics = display_advanced_metrics(filtered_repo_data, filtered_issues_data, filtered_pr_data)
        st.plotly_chart(advanced_metrics, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(visualizations[5], use_container_width=True)
        with col2:
            st.plotly_chart(visualizations[8], use_container_width=True)

    # Footer
    st.markdown("---")
    st.markdown("<p style='text-align: center;'>Created with ❤️ by Mebarek</p>", unsafe_allow_html=True)

    # Data export option
    with st.expander("Export Data"):
        st.markdown("### Download Filtered Data")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(get_download_link(filtered_repo_data, 'filtered_repo_data.csv', 'Repository Data'),
                        unsafe_allow_html=True)
        with col2:
            st.markdown(get_download_link(filtered_issues_data, 'filtered_issues_data.csv', 'Issues Data'),
                        unsafe_allow_html=True)
        with col3:
            st.markdown(get_download_link(filtered_pr_data, 'filtered_pr_data.csv', 'PR Data'), unsafe_allow_html=True)

    # Sidebar Footer
    st.sidebar.markdown("---")
    st.sidebar.write(
        '<p style="text-align: center;">Created with ❤️ by <a href="https://www.linkedin.com/in/mohammed-mecheter/">Mebarek</a>!</p>',
        unsafe_allow_html=True)

    # Load Lottie animations
    lottie_files = load_lottie_files()

    # Sidebar section with Lottie animations and links
    with st.sidebar:
        st.markdown("### Connect with me")

        for icon, link in zip(['github', 'linkedin', 'profile'],
                              ['https://github.com/Mohammed-Mebarek-Mecheter/',
                               'https://www.linkedin.com/in/mohammed-mecheter/',
                               'https://mebarek.pages.dev/']):
            col1, col2 = st.columns([1, 3])
            with col1:
                st_lottie(lottie_files[icon], height=30, width=30, key=f"lottie_{icon}_sidebar")
            with col2:
                st.markdown(f"<a href='{link}' target='_blank'>{icon.capitalize()}</a>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()