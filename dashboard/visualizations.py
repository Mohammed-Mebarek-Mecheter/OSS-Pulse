import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def plot_repository_growth(repo_data):
    """
    Plot repository growth over time (stars, forks, issues).

    Parameters:
    - repo_data: DataFrame with repository data.
    """
    st.subheader("Repository Growth Over Time")

    # Convert 'created_at' to datetime if it's not already
    repo_data['created_at'] = pd.to_datetime(repo_data['created_at'])

    # Group by month and calculate cumulative sum
    monthly_data = repo_data.groupby(repo_data['created_at'].dt.to_period('M')).agg({
        'stars': 'sum',
        'forks': 'sum',
        'open_issues': 'sum'
    }).reset_index()
    monthly_data['created_at'] = monthly_data['created_at'].dt.to_timestamp()
    monthly_data = monthly_data.sort_values('created_at')
    monthly_data['cumulative_stars'] = monthly_data['stars'].cumsum()
    monthly_data['cumulative_forks'] = monthly_data['forks'].cumsum()
    monthly_data['cumulative_issues'] = monthly_data['open_issues'].cumsum()

    fig = px.line(
        monthly_data,
        x='created_at',
        y=['cumulative_stars', 'cumulative_forks', 'cumulative_issues'],
        labels={'value': 'Count', 'created_at': 'Date'},
        title='Cumulative Growth of Stars, Forks, and Open Issues'
    )
    st.plotly_chart(fig)

def plot_issue_resolution_time(issues_data):
    """
    Plot the issue resolution time for repositories.

    Parameters:
    - issues_data: DataFrame with issues data.
    """
    st.subheader("Issue Resolution Time")

    # Filter out unresolved issues
    resolved_issues = issues_data[issues_data['resolution_time_days'] >= 0]

    fig = px.box(
        resolved_issues,
        x='repository',
        y='resolution_time_days',
        title='Issue Resolution Time by Repository',
        labels={'resolution_time_days': 'Resolution Time (days)', 'repository': 'Repository'},
        points="all"
    )
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(fig)

def plot_pull_request_merge_time(pr_data):
    """
    Plot pull request merge time across repositories.

    Parameters:
    - pr_data: DataFrame with pull request data.
    """
    st.subheader("Pull Request Merge Time")

    # Filter out unmerged PRs
    merged_prs = pr_data[pr_data['merge_time_days'] >= 0]

    fig = px.box(
        merged_prs,
        x='repository',
        y='merge_time_days',
        title='Pull Request Merge Time by Repository',
        labels={'merge_time_days': 'Merge Time (days)', 'repository': 'Repository'},
        points="all"
    )
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(fig)

def plot_contributor_activity(repo_data):
    """
    Plot the number of contributors for each repository.

    Parameters:
    - repo_data: DataFrame with repository data.
    """
    st.subheader("Contributor Activity")

    # Sort repositories by total contributors
    top_repos = repo_data.sort_values('total_contributors', ascending=False).head(20)

    fig = px.bar(
        top_repos,
        x='full_name',
        y='total_contributors',
        title='Top 20 Repositories by Total Contributors',
        labels={'total_contributors': 'Total Contributors', 'full_name': 'Repository'}
    )
    fig.update_layout(xaxis={'tickangle': 45})
    st.plotly_chart(fig)

def plot_repository_size_distribution(repo_data):
    """
    Plot the distribution of repository sizes.

    Parameters:
    - repo_data: DataFrame with repository data.
    """
    st.subheader("Repository Size Distribution")

    size_distribution = repo_data['size_category'].value_counts().sort_index()

    fig = px.bar(
        x=size_distribution.index,
        y=size_distribution.values,
        labels={'x': 'Size Category', 'y': 'Count'},
        title='Distribution of Repository Sizes'
    )
    st.plotly_chart(fig)

def plot_correlation_heatmap(repo_data):
    """
    Plot a correlation heatmap of repository metrics.

    Parameters:
    - repo_data: DataFrame with repository data.
    """
    st.subheader("Correlation Heatmap of Repository Metrics")

    metrics = ['stars', 'forks', 'open_issues', 'total_contributors']
    corr_matrix = repo_data[metrics].corr()

    fig = px.imshow(
        corr_matrix,
        labels=dict(color="Correlation"),
        x=metrics,
        y=metrics,
        color_continuous_scale='RdBu_r',
        title='Correlation Heatmap of Repository Metrics'
    )
    st.plotly_chart(fig)