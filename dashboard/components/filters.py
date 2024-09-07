# filters.py
import streamlit as st
import pandas as pd

@st.cache_data
def apply_advanced_filters(repo_data, issues_data, pr_data):
    st.sidebar.subheader("Advanced Filters")

    # Filter by stars
    min_stars = int(repo_data['stars'].min())
    max_stars = int(repo_data['stars'].max())

    # Check if min_stars equals max_stars to prevent slider error
    if min_stars == max_stars:
        st.sidebar.write(f"All repositories have {min_stars} stars")
        star_range = (min_stars, max_stars)
    else:
        star_range = st.sidebar.slider(
            "Filter by Stars",
            min_value=min_stars,
            max_value=max_stars,
            value=(min_stars, max_stars)
        )

    repo_data = repo_data[(repo_data['stars'] >= star_range[0]) & (repo_data['stars'] <= star_range[1])]

    # Filter by forks
    min_forks = int(repo_data['forks'].min())
    max_forks = int(repo_data['forks'].max())

    # Check if min_forks equals max_forks
    if min_forks == max_forks:
        st.sidebar.write(f"All repositories have {min_forks} forks")
        fork_range = (min_forks, max_forks)
    else:
        fork_range = st.sidebar.slider(
            "Filter by Forks",
            min_value=min_forks,
            max_value=max_forks,
            value=(min_forks, max_forks)
        )

    repo_data = repo_data[(repo_data['forks'] >= fork_range[0]) & (repo_data['forks'] <= fork_range[1])]

    # Filter by open issues
    min_issues = int(repo_data['open_issues'].min())
    max_issues = int(repo_data['open_issues'].max())

    # Check if min_issues equals max_issues
    if min_issues == max_issues:
        st.sidebar.write(f"All repositories have {min_issues} open issues")
        issue_range = (min_issues, max_issues)
    else:
        issue_range = st.sidebar.slider(
            "Filter by Open Issues",
            min_value=min_issues,
            max_value=max_issues,
            value=(min_issues, max_issues)
        )

    repo_data = repo_data[(repo_data['open_issues'] >= issue_range[0]) & (repo_data['open_issues'] <= issue_range[1])]

    # Filter by contributors
    if 'total_contributors' in repo_data.columns:
        min_contributors = int(repo_data['total_contributors'].min())
        max_contributors = int(repo_data['total_contributors'].max())

        # Check if min_contributors equals max_contributors
        if min_contributors == max_contributors:
            st.sidebar.write(f"All repositories have {min_contributors} contributors")
            contributor_range = (min_contributors, max_contributors)
        else:
            contributor_range = st.sidebar.slider(
                "Filter by Contributors",
                min_value=min_contributors,
                max_value=max_contributors,
                value=(min_contributors, max_contributors)
            )

        repo_data = repo_data[(repo_data['total_contributors'] >= contributor_range[0]) & (
                    repo_data['total_contributors'] <= contributor_range[1])]

    # Filter issues and PRs based on the filtered repositories
    filtered_repo_names = repo_data['name'].unique()
    issues_data = issues_data[issues_data['repository'].isin(filtered_repo_names)]
    pr_data = pr_data[pr_data['repository'].isin(filtered_repo_names)]

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

    # Return the filtered data after all filters have been applied
    return repo_data, issues_data, pr_data
