# sidebar.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from streamlit_lottie import st_lottie
import json

@st.cache_data
def display_sidebar(repo_data, issues_data, pr_data):
    st.sidebar.header("Filter Options")

    filters = {}

    # Filter by repository name or full name
    search_term = st.sidebar.text_input("Search by Repository Name or Full Name", "")
    if search_term:
        filters['search_term'] = search_term

    # Filter by date range
    try:
        repo_data['created_at'] = pd.to_datetime(repo_data['created_at'], errors='coerce')
        min_date = repo_data['created_at'].min().date()
        max_date = repo_data['created_at'].max().date()
    except Exception as e:
        st.sidebar.error(f"Error parsing dates: {str(e)}")
        st.sidebar.error("Using default date range due to parsing errors.")
        min_date = datetime(2000, 1, 1).date()
        max_date = datetime.now().date()

    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if len(date_range) == 2:
        filters['date_range'] = date_range
    else:
        st.sidebar.warning("Please select both start and end dates.")
        filters['date_range'] = (min_date, max_date)

    # Repository size category filter
    size_categories = repo_data['size_category'].unique().tolist()
    filters['selected_category'] = st.sidebar.multiselect(
        "Select Repository Size Categories",
        options=size_categories,
        default=size_categories
    )

    # Stars and Forks range filters
    col1, col2 = st.sidebar.columns(2)
    with col1:
        filters['star_range'] = st.slider(
            "Stars Range",
            min_value=int(repo_data['stars'].min()),
            max_value=int(repo_data['stars'].max()),
            value=(int(repo_data['stars'].min()), int(repo_data['stars'].max()))
        )
    with col2:
        filters['fork_range'] = st.slider(
            "Forks Range",
            min_value=int(repo_data['forks'].min()),
            max_value=int(repo_data['forks'].max()),
            value=(int(repo_data['forks'].min()), int(repo_data['forks'].max()))
        )

    # Issue resolution time filter
    filters['issue_resolution_time'] = st.sidebar.slider(
        "Issue Resolution Time (days)",
        min_value=0,
        max_value=int(issues_data['resolution_time_days'].max()),
        value=(0, int(issues_data['resolution_time_days'].max()))
    )

    # PR merge time filter
    filters['pr_merge_time'] = st.sidebar.slider(
        "PR Merge Time (days)",
        min_value=0,
        max_value=int(pr_data['merge_time_days'].max()),
        value=(0, int(pr_data['merge_time_days'].max()))
    )

    # Stale repository filter
    filters['include_stale'] = st.sidebar.checkbox("Include Stale Repositories", value=True)

    return filters

@st.cache_data
def apply_filters(repo_data, issues_data, pr_data, filters):
    # Apply search filter on repository name or full name
    if 'search_term' in filters and filters['search_term']:
        search_term = filters['search_term'].lower()
        repo_data = repo_data[
            repo_data['name'].str.contains(search_term, case=False, na=False) |
            repo_data['full_name'].str.contains(search_term, case=False, na=False)
            ]

    # Apply date range filter
    start_date, end_date = filters['date_range']
    repo_data = repo_data[
        (repo_data['created_at'].dt.date >= start_date) &
        (repo_data['created_at'].dt.date <= end_date)
        ]

    # Apply size category filter
    if filters['selected_category']:
        repo_data = repo_data[repo_data['size_category'].isin(filters['selected_category'])]

    # Apply stars and forks range filters
    repo_data = repo_data[
        (repo_data['stars'] >= filters['star_range'][0]) &
        (repo_data['stars'] <= filters['star_range'][1]) &
        (repo_data['forks'] >= filters['fork_range'][0]) &
        (repo_data['forks'] <= filters['fork_range'][1])
        ]

    # Apply stale repository filter
    if not filters['include_stale']:
        repo_data = repo_data[~repo_data['stale']]

    # Filter issues and PRs based on the filtered repositories
    filtered_repo_names = repo_data['name'].unique()
    issues_data = issues_data[issues_data['repository'].isin(filtered_repo_names)]
    pr_data = pr_data[pr_data['repository'].isin(filtered_repo_names)]

    return repo_data, issues_data, pr_data

    # Sidebar Footer
    st.sidebar.markdown("---")
    st.sidebar.write("Developed by **Mebarek**")

    def load_lottie_file(filepath: str):
        with open(filepath, "r") as f:
            return json.load(f)

    # Load Lottie animations
    lottie_github = load_lottie_file("dashboard/assets/images/github.json")
    lottie_linkedin = load_lottie_file("dashboard/assets/images/linkedin.json")
    lottie_portfolio = load_lottie_file("dashboard/assets/images/profile.json")

    # Sidebar section with Lottie animations and links
    with st.sidebar:
        st.markdown("### Connect with me")

        col1, col2 = st.columns([1, 3])
        with col1:
            st_lottie(lottie_github, height=30, width=30, key="lottie_github_sidebar")
        with col2:
            st.markdown("<a href='https://github.com/Mohammed-Mebarek-Mecheter/' target='_blank'>GitHub</a>",
                        unsafe_allow_html=True)

        col1, col2 = st.columns([1, 3])
        with col1:
            st_lottie(lottie_linkedin, height=30, width=30, key="lottie_linkedin_sidebar")
        with col2:
            st.markdown("<a href='https://www.linkedin.com/in/mohammed-mecheter/' target='_blank'>LinkedIn</a>",
                        unsafe_allow_html=True)

        col1, col2 = st.columns([1, 3])
        with col1:
            st_lottie(lottie_portfolio, height=30, width=30, key="lottie_portfolio_sidebar")
        with col2:
            st.markdown("<a href='https://mebarek.pages.dev/' target='_blank'>Portfolio</a>", unsafe_allow_html=True)
