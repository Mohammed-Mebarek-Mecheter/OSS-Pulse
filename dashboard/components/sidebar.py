import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

def display_sidebar(repo_data):
    """
    Renders the sidebar with filters for repository data.

    Parameters:
    - repo_data: DataFrame with repository data to filter.

    Returns:
    - Dict containing all selected filter options.
    """
    st.sidebar.header("Filter Options")

    filters = {}

    # Filter by repository size category
    size_categories = repo_data['size_category'].unique().tolist()
    filters['selected_category'] = st.sidebar.multiselect(
        "Select Repository Size Categories",
        options=size_categories,
        default=size_categories
    )

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

    # Filter by number of stars
    max_stars = int(repo_data['stars'].max())
    filters['star_range'] = st.sidebar.slider(
        "Stars Range",
        min_value=0,
        max_value=max_stars,
        value=(0, max_stars)
    )

    # Filter by number of forks
    max_forks = int(repo_data['forks'].max())
    filters['fork_range'] = st.sidebar.slider(
        "Forks Range",
        min_value=0,
        max_value=max_forks,
        value=(0, max_forks)
    )

    # Filter stale repositories
    filters['include_stale'] = st.sidebar.checkbox("Include Stale Repositories", value=True)

    return filters

def apply_filters(repo_data, filters):
    """
    Applies the selected filters to the repository data.

    Parameters:
    - repo_data: DataFrame with repository data.
    - filters: Dict containing filter options.

    Returns:
    - Filtered DataFrame.
    """
    filtered_data = repo_data.copy()

    # Apply size category filter
    if filters['selected_category']:
        filtered_data = filtered_data[filtered_data['size_category'].isin(filters['selected_category'])]

    # Apply date range filter
    start_date, end_date = filters['date_range']
    filtered_data = filtered_data[
        (filtered_data['created_at'].dt.date >= start_date) &
        (filtered_data['created_at'].dt.date <= end_date)
    ]

    # Apply star range filter
    min_stars, max_stars = filters['star_range']
    filtered_data = filtered_data[
        (filtered_data['stars'] >= min_stars) &
        (filtered_data['stars'] <= max_stars)
    ]

    # Apply fork range filter
    min_forks, max_forks = filters['fork_range']
    filtered_data = filtered_data[
        (filtered_data['forks'] >= min_forks) &
        (filtered_data['forks'] <= max_forks)
    ]

    # Apply stale repository filter
    if not filters['include_stale']:
        filtered_data = filtered_data[~filtered_data['stale']]

    return filtered_data