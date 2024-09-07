# visualizations.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

@st.cache_data
def plot_repository_growth(repo_data):
    repo_data['created_at'] = pd.to_datetime(repo_data['created_at'])
    monthly_data = repo_data.resample('ME', on='created_at').size().reset_index(name='count')
    monthly_data['cumulative_count'] = monthly_data['count'].cumsum()

    fig = px.line(monthly_data, x='created_at', y='cumulative_count',
                  title='Cumulative Repository Growth')
    return fig

@st.cache_data
def plot_issue_resolution_time(issues_data):
    resolved_issues = issues_data[issues_data['resolution_time_days'] >= 0]
    fig = px.box(resolved_issues, x='repository', y='resolution_time_days',
                 title='Issue Resolution Time by Repository')
    fig.update_layout(xaxis={'categoryorder': 'total descending'})
    return fig

@st.cache_data
def plot_pull_request_merge_time(pr_data):
    merged_prs = pr_data[pr_data['merge_time_days'] >= 0]
    fig = px.box(merged_prs, x='repository', y='merge_time_days',
                 title='Pull Request Merge Time by Repository')
    fig.update_layout(xaxis={'categoryorder': 'total descending'})
    return fig

@st.cache_data
def plot_contributor_activity(repo_data):
    top_repos = repo_data.nlargest(20, 'total_contributors')
    fig = px.bar(top_repos, x='name', y='total_contributors',
                 title='Top 20 Repositories by Total Contributors')
    fig.update_layout(xaxis={'categoryorder': 'total descending'})
    return fig

@st.cache_data
def plot_repository_size_distribution(repo_data):
    size_distribution = repo_data['size_category'].value_counts().sort_index()
    fig = px.pie(values=size_distribution.values, names=size_distribution.index,
                 title="Repository Size Distribution")
    return fig

@st.cache_data
def plot_correlation_heatmap(repo_data):
    metrics = ['stars', 'forks', 'open_issues', 'total_contributors']
    corr_matrix = repo_data[metrics].corr()
    fig = px.imshow(corr_matrix, text_auto=True, aspect="auto")
    fig.update_layout(title="Correlation Heatmap of Repository Metrics")
    return fig

@st.cache_data
def plot_issue_pr_funnel(issues_data, pr_data):
    open_issues = len(issues_data[issues_data['state'] == 'open'])
    closed_issues = len(issues_data[issues_data['state'] == 'closed'])
    open_prs = len(pr_data[pr_data['state'] == 'open'])
    merged_prs = len(pr_data[pr_data['state'] == 'merged'])

    fig = go.Figure(go.Funnel(
        y=['Open Issues', 'Closed Issues', 'Open PRs', 'Merged PRs'],
        x=[open_issues, closed_issues, open_prs, merged_prs],
        textinfo="value+percent initial"
    ))

    fig.update_layout(title_text="Issue to PR Funnel")

    return fig

@st.cache_data
def plot_trend_analysis(repo_data):
    """
    Plots trend analysis for repository growth and engagement metrics.
    """
    repo_data['created_at'] = pd.to_datetime(repo_data['created_at'])
    monthly_data = repo_data.resample('ME', on='created_at').agg({
        'id': 'count',
        'stars': 'sum',
        'forks': 'sum'
    }).reset_index()
    monthly_data.columns = ['date', 'new_repos', 'new_stars', 'new_forks']

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=monthly_data['date'], y=monthly_data['new_repos'], name='New Repositories'))
    fig.add_trace(go.Scatter(x=monthly_data['date'], y=monthly_data['new_stars'], name='New Stars', yaxis='y2'))
    fig.add_trace(go.Scatter(x=monthly_data['date'], y=monthly_data['new_forks'], name='New Forks', yaxis='y3'))

    fig.update_layout(
        title='Monthly Trends: New Repositories, Stars, and Forks',
        xaxis=dict(title='Date'),
        yaxis=dict(title='New Repositories', side='left'),
        yaxis2=dict(title='New Stars', side='right', overlaying='y', showgrid=False),
        yaxis3=dict(title='New Forks', side='right', overlaying='y', showgrid=False, anchor='free', position=1),
        legend=dict(x=1.1, y=1)
    )

    return fig

@st.cache_data
def plot_repository_treemap(repo_data):
    """
    Creates a treemap visualization of repositories based on stars and size category.
    """
    fig = px.treemap(repo_data, path=['size_category', 'name'], values='stars',
                     color='stars', hover_data=['forks', 'open_issues'],
                     color_continuous_scale='blues',
                     title='Repository Treemap by Stars and Size Category')
    return fig

@st.cache_data
def plot_issues_vs_prs(repo_data, issues_data, pr_data):
    """
    Creates a scatter plot comparing issues and pull requests for repositories.
    """
    repo_summary = repo_data.merge(
        issues_data.groupby('repository').size().rename('issue_count'),
        left_on='name', right_index=True, how='left'
    ).merge(
        pr_data.groupby('repository').size().rename('pr_count'),
        left_on='name', right_index=True, how='left'
    )

    fig = px.scatter(repo_summary, x='issue_count', y='pr_count', color='stars',
                     size='total_contributors', hover_data=['name', 'id'],
                     labels={'issue_count': 'Number of Issues', 'pr_count': 'Number of PRs'},
                     title='Issues vs Pull Requests by Repository')
    return fig

@st.cache_data
def display_top_repositories(repo_data, top_n=10):
    top_repos = repo_data.nlargest(top_n, 'stars')

    # Create a bar chart
    fig = px.bar(top_repos, x='name', y='stars',
                 title=f'Top {top_n} Repositories by Stars',
                 labels={'name': 'Repository', 'stars': 'Stars'},
                 hover_data=['forks', 'open_issues'])

    # Customize the layout
    fig.update_layout(xaxis_tickangle=-45, xaxis_title="", yaxis_title="Stars")

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)