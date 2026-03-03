# This script sets up a basic pipeline for processing CSV files in a specified directory.
# It loads the CSVs into a pandas DataFrame, processes the data, and writes unique IDs to a file.

import pandas as pd
import os
import time
import webbrowser
import math
from pathlib import Path
import plotly.graph_objects as go
import plotly.io as pio


# Global color palette for consistent tag coloring across all visualizations
TAG_COLOR_PALETTE = [
    '#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231',
    '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe',
    '#008080', '#e6beff', '#9a6324', '#fffac8', '#800000',
    '#aaffc3', '#808000', '#ffd8b1', '#000075', '#808080',
    '#ff6347', '#40e0d0', '#ff1493', '#00ced1', '#ff8c00',
    '#9370db', '#00fa9a', '#dc143c', '#00bfff', '#adff2f'
]


def get_tag_color_map(tags):
    """
    Creates a consistent color mapping for tags.
    Tags are sorted alphabetically to ensure consistency across all charts.
    
    Args:
        tags: List or set of tag names
    
    Returns:
        Dictionary mapping tag names to rgba color strings
    """
    sorted_tags = sorted(set(tags))
    color_map = {}
    
    for i, tag in enumerate(sorted_tags):
        color_hex = TAG_COLOR_PALETTE[i % len(TAG_COLOR_PALETTE)]
        # Convert hex to rgba
        r = int(color_hex[1:3], 16)
        g = int(color_hex[3:5], 16)
        b = int(color_hex[5:7], 16)
        color_map[tag] = f'rgba({r},{g},{b},0.8)'
    
    return color_map


def save_and_open_html(fig, output_file, chart_label='Visualization', post_script=None):
    if post_script is None:
        fig.write_html(output_file, include_plotlyjs=True, auto_open=False)
    else:
        pio.write_html(fig, output_file, post_script=post_script, include_plotlyjs=True, auto_open=False)

    html_path = Path(output_file).resolve()
    html_uri = html_path.as_uri()
    print(f"{chart_label} saved to {output_file}")

    for attempt in range(1, 4):
        opened = webbrowser.open_new_tab(html_uri)
        if opened:
            return
        time.sleep(0.5 * attempt)

    print(f"Could not auto-open {output_file}. Open it manually from: {html_path}")

# Function to load CSV files from a specified directory

def load_csv_files(directory):
    dataframes = []
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    separators = [';', ',']
    
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            
            # Try different encodings and separators
            loaded = False
            for encoding in encodings:
                if loaded:
                    break
                for separator in separators:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding, sep=separator)
                        # Check if file was parsed correctly (more than 1 column usually means correct separator)
                        if len(df.columns) > 1:
                            print(f"Loaded {filename} with {encoding} encoding and '{separator}' separator")
                            dataframes.append(df)
                            loaded = True
                            break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        continue
            
            if not loaded:
                print(f"Failed to load {filename}")
                
    return dataframes

# Function to create a DataFrame from loaded CSVs

def create_dataframe(dataframes):
    combined_df = pd.concat(dataframes, ignore_index=True)
    # Remove duplicate rows, keeping the first occurrence
    combined_df = combined_df.drop_duplicates(keep='first')
    return combined_df

# Function to print column names and hardcode id and description columns

def process_dataframe(df):
    print("Column names:", df.columns.tolist())
    # Hardcoding id and description columns. look at the column names after the first run.
    id_name = 'Naam / Omschrijving'
    account = 'Rekening'
    value_col = 'Bedrag (EUR)'
    value_sign_col = 'Af Bij'
    sign_subtract = 'Af'
    time_col = 'Datum'
    balance_col = 'Saldo na mutatie'
    df['id'] = df[id_name].astype(str)
    # Convert comma decimal separator to dot, then to float
    df['value'] = df[value_col].astype(str).str.replace(',', '.').astype(float) * df[value_sign_col].apply(lambda x: -1 if x.strip().lower() == sign_subtract.lower() else 1)
    df['account'] = df[account].astype(str)
    df['balance'] = df[balance_col].astype(str).str.replace(',', '.').astype(float)
    
    # Detect and parse date format
    sample_date = str(df[time_col].iloc[0])
    
    # Check if date has no separators (like 20250812)
    if len(sample_date) == 8 and sample_date.isdigit():
        df['time'] = pd.to_datetime(df[time_col].astype(str), format='%Y%m%d', errors='coerce')
        print(f"Detected date format: yyyymmdd (no separator)")
    # Check if date uses / separator
    elif '/' in sample_date:
        df['time'] = pd.to_datetime(df[time_col], format='%Y/%m/%d', errors='coerce')
        print(f"Detected date format: yyyy/mm/dd")
    # Check if date uses - separator
    elif '-' in sample_date:
        df['time'] = pd.to_datetime(df[time_col], format='%Y-%m-%d', errors='coerce')
        print(f"Detected date format: yyyy-mm-dd")
    else:
        # Fallback to pandas auto-detection
        df['time'] = pd.to_datetime(df[time_col], errors='coerce')
        print(f"Using automatic date detection")
    
    # Remove duplicates based on time, id, and value (keep first occurrence)
    initial_count = len(df)
    df = df.drop_duplicates(subset=['time', 'id', 'value'], keep='first')
    removed_duplicates = initial_count - len(df)
    if removed_duplicates > 0:
        print(f"Removed {removed_duplicates} duplicate rows based on time, id, and value")
    
    return df

# Function to write unique ids to a tags CSV file

def write_unique_ids(df, tags_file):
    # Calculate sum of values and collect accounts per ID
    id_summary = df.groupby('id').agg({
        'value': 'sum',
        'account': lambda x: ', '.join(sorted(x.unique()))
    }).reset_index()
    id_summary = id_summary.sort_values('id')
    
    existing_tags = {}
    
    if os.path.exists(tags_file):
        # Load existing tags
        try:
            tags_df = pd.read_csv(tags_file, sep=';', encoding='utf-8')
            existing_tags = dict(zip(tags_df['id'], tags_df['tag']))
            print(f"Loaded {len(existing_tags)} existing tags from {tags_file}")
        except Exception as e:
            print(f"Could not load existing tags: {e}")
    
    # Create tags dataframe with existing or empty tags and sums
    tags_data = []
    for _, row in id_summary.iterrows():
        uid = row['id']
        tag = existing_tags.get(uid, '')  # Use existing tag or empty string
        tags_data.append({
            'id': uid, 
            'sum_value': round(row['value'], 2),
            'accounts': row['account'],
            'tag': tag
        })
    
    # Write to CSV file
    tags_output_df = pd.DataFrame(tags_data)
    tags_output_df.to_csv(tags_file, sep=';', index=False, encoding='utf-8')
    print(f"Written {len(tags_data)} unique IDs to {tags_file}")
    print(f"You can now edit {tags_file} to add tags for each ID")
    
    return tags_output_df

# Function to load tags and merge with dataframe

def apply_tags_to_dataframe(df, tags_file):
    df = df.copy()

    # Load tags from file
    if os.path.exists(tags_file):
        tags_df = pd.read_csv(tags_file, sep=';', encoding='utf-8')
        # Create mapping from id to tag, default empty tags to 'other'
        tag_mapping = {row['id']: row['tag'] if pd.notna(row['tag']) and row['tag'].strip() != '' else 'other'
                      for _, row in tags_df.iterrows()}
    else:
        print(f"Tags file {tags_file} not found, all tags will be 'other'")
        tag_mapping = {}
    
    # Apply tags to dataframe, default to 'other' if not found
    df.loc[:, 'tag'] = (
        df['id']
        .map(tag_mapping)
        .fillna('other')
        .astype(str)
        .str.strip()
        .replace('', 'other')
        .str.lower()
    )
    
    # Print unique tags
    unique_tags = sorted(df['tag'].unique())
    print(f"\nUnique tags found: {unique_tags}")
    print(f"Total unique tags: {len(unique_tags)}")
    
    return df

# Function to create interactive Plotly visualization

def visualize_finances_interactive(df, accumulate_tag=None):
    # Sort by time to ensure chronological order
    df = df.sort_values('time')
    
    # Group by time and tag (net value per month & tag)
    monthly_data = df.groupby([pd.Grouper(key='time', freq='ME'), 'tag'])['value'].sum().unstack(fill_value=0)
    
    # Get starting balance for each month
    monthly_balance = df.groupby(pd.Grouper(key='time', freq='ME'))['balance'].first()
    
    # Use starting balance values directly (aligned with monthly_data rows)
    starting_balances = list(monthly_balance.values)

    # Define a stable tag order based on total absolute movement (largest first)
    tag_order = (
        monthly_data
        .abs()
        .sum()
        .sort_values(ascending=False)
        .index
        .tolist()
    )

    # Get consistent color mapping for all tags
    colors = get_tag_color_map(tag_order)
    
    # Create figure
    fig = go.Figure()

    # Calculate month labels
    months = monthly_data.index.strftime('%Y-%m')

    # For each tag, stack values per month based on sign:
    # - positive values stack upward from the starting balance
    # - negative values stack downward from the starting balance
    for tag in tag_order:
        base_values = []
        heights = []

        # Precompute position of this tag in the stacking order
        tag_pos = tag_order.index(tag)

        for i, month in enumerate(monthly_data.index):
            value = monthly_data.loc[month, tag]

            if value > 0:
                # Sum all positive values of tags that come before this tag
                cumulative_pos = sum(
                    monthly_data.loc[month, t]
                    for t in tag_order[:tag_pos]
                    if monthly_data.loc[month, t] > 0
                )
                base = starting_balances[i] + cumulative_pos
            elif value < 0:
                # Sum all negative values of tags that come before this tag
                cumulative_neg = sum(
                    monthly_data.loc[month, t]
                    for t in tag_order[:tag_pos]
                    if monthly_data.loc[month, t] < 0
                )
                base = starting_balances[i] + cumulative_neg
            else:
                # No bar for zero values in this month
                base = None

            base_values.append(base)
            heights.append(value)

        fig.add_trace(go.Bar(
            name=tag,
            x=months,
            y=heights,
            base=base_values,
            marker_color=colors.get(tag, 'gray'),
            customdata=heights,
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Month: %{x}<br>' +
                         'Amount: €%{customdata:.2f}<br>' +
                         '<extra></extra>',
        ))

    # Optional: add a cumulative line for a specific tag
    if accumulate_tag is not None:
        if accumulate_tag not in monthly_data.columns:
            print(f"accumulate_tag '{accumulate_tag}' not found in tags. Available tags: {list(monthly_data.columns)}")
        else:
            tag_series = monthly_data[accumulate_tag].sort_index()
            cumulative_values = tag_series.cumsum()

            fig.add_trace(go.Scatter(
                name=f"{accumulate_tag} (cumulative)",
                x=months,
                y=cumulative_values.values,
                mode='lines+markers',
                line=dict(color=colors.get(accumulate_tag, 'black'), width=3, dash='dot'),
                marker=dict(size=7, color=colors.get(accumulate_tag, 'black')),
                hovertemplate='<b>%{fullData.name}</b><br>' +
                             'Month: %{x}<br>' +
                             'Cumulative total: €%{y:.2f}<br>' +
                             '<extra></extra>',
                legendgroup='Cumulative',
                showlegend=True,
            ))

    # Add balance line (starting balance per month)
    fig.add_trace(go.Scatter(
        name='Starting Balance',
        x=months,
        y=monthly_balance.values,
        mode='lines+markers',
        line=dict(color='black', width=3),
        marker=dict(size=8),
        hovertemplate='<b>Starting Balance</b><br>' +
                     'Month: %{x}<br>' +
                     'Balance: €%{y:.2f}<br>' +
                     '<extra></extra>',
        legendgroup='Balance',
        showlegend=True
    ))
    
    # Update layout
    fig.update_layout(
        title='Income and Expenses Over Time by Tag (Interactive Waterfall)',
        xaxis_title='Month',
        yaxis_title='Amount (EUR)',
        barmode='stack',
        hovermode='closest',
        hoverdistance=20,
        width=1400,
        height=800,
        legend=dict(
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02,
            groupclick='toggleitem'
        )
    )
    
    # Update hover behavior
    fig.update_traces(
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial"
        )
    )
    
    # Save and open
    save_and_open_html(
        fig,
        'financial_visualization_interactive.html',
        chart_label='Interactive visualization'
    )


# Function to create a small monthly expenses line chart

def visualize_monthly_expenses_line(df):
    # Keep only expenses (negative values)
    df_exp = df[df['value'] < 0].copy()
    if df_exp.empty:
        print("No expenses found to plot for monthly expenses line chart.")
        return

    # Aggregate expenses per month and tag; store positive amounts for readability
    monthly_expenses = (
        df_exp
        .groupby([pd.Grouper(key='time', freq='M'), 'tag'])['value']
        .sum()
        .reset_index()
    )

    # Use month as a timestamp and flip sign so expenses are positive in the chart
    monthly_expenses['month'] = monthly_expenses['time'].dt.to_period('M').dt.to_timestamp()
    monthly_expenses['amount'] = -monthly_expenses['value']

    fig = go.Figure()

    # Order tags by total expenses (descending) for a stable legend order
    tag_totals = (
        monthly_expenses.groupby('tag')['amount']
        .sum()
        .sort_values(ascending=False)
    )

    # Get consistent color mapping
    colors = get_tag_color_map(tag_totals.index)

    for tag in tag_totals.index:
        tag_data = monthly_expenses[monthly_expenses['tag'] == tag]
        fig.add_trace(go.Scatter(
            name=tag,
            x=tag_data['month'],
            y=tag_data['amount'],
            mode='lines+markers',
            line=dict(color=colors[tag]),
            marker=dict(color=colors[tag]),
            hoverinfo='none',
        ))

    fig.update_layout(
        title='Monthly Expenses by Tag (Line)',
        xaxis_title='Month',
        yaxis_title='Expenses (EUR, positive = outflow)',
        hovermode='x',
        autosize=True,
        xaxis=dict(autorange=True),
        yaxis=dict(autorange=True),
        legend=dict(
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02,
        )
    )

    output_file = 'monthly_expenses_line.html'
    post_script = """
var plot = document.getElementById('{plot_id}');
if (plot) {
    plot.on('plotly_hover', function(data) {
        var points = (data && data.points) ? data.points : [];
        if (!points.length) return;
        var point = points[0];
        var month = point.x;
        var items = points
            .filter(function(p) { return p.data && p.data.name; })
            .map(function(p) { return { name: p.data.name, y: p.y }; });
        items.sort(function(a, b) { return b.y - a.y; });
        var lines = items.map(function(i) { return i.name + ': €' + i.y.toFixed(2); });
        var text = 'Month: ' + month + '<br>' + lines.join('<br>');
        var ann = [{
            xref: 'x', yref: 'y', x: point.x, y: point.y,
            xanchor: 'right', yanchor: 'bottom', showarrow: false,
            xshift: 0, yshift: 0,
            text: text, bgcolor: 'white', bordercolor: '#333', borderwidth: 1,
            font: { size: 12 }
        }];
        Plotly.relayout(plot, { annotations: ann });
    });
    plot.on('plotly_unhover', function() {
        Plotly.relayout(plot, { annotations: [] });
    });
}
"""
    save_and_open_html(
        fig,
        output_file,
        chart_label='Monthly expenses line visualization',
        post_script=post_script
    )


# Function to create circular diagram showing average daily spending per year

def visualize_daily_spending_circle(df, year=None):
    """
    Creates a circular/polar diagram showing average spending per day of year.
    If year is None, uses the most recent year with data.
    """
    # Filter for expenses only (negative values)
    df_exp = df[df['value'] < 0].copy()
    if df_exp.empty:
        print("No expenses found for circular daily spending visualization.")
        return
    
    # Determine which year to visualize
    if year is None:
        year = df_exp['time'].dt.year.max()
    
    # Filter for the selected year
    df_year = df_exp[df_exp['time'].dt.year == year].copy()
    
    if df_year.empty:
        print(f"No expenses found for year {year}.")
        return
    
    # Add day of year (1-365/366)
    df_year['day_of_year'] = df_year['time'].dt.dayofyear
    df_year['month_name'] = df_year['time'].dt.strftime('%b')
    df_year['date_str'] = df_year['time'].dt.strftime('%Y-%m-%d')
    
    # Flip sign so expenses are positive
    df_year['amount'] = -df_year['value']
    
    # Group by day of year and tag
    daily_spending = (
        df_year.groupby(['day_of_year', 'date_str', 'tag'])['amount']
        .sum()
        .reset_index()
    )
    
    # Calculate total daily spending (across all tags)
    daily_totals = (
        df_year.groupby(['day_of_year', 'date_str'])['amount']
        .sum()
        .reset_index()
    )
    
    # Get tag totals for ordering
    tag_totals = (
        daily_spending.groupby('tag')['amount']
        .sum()
        .sort_values(ascending=False)
    )
    
    # Get consistent color mapping
    colors = get_tag_color_map(tag_totals.index)
    
    # Create polar scatter plot
    fig = go.Figure()
    
    # Add traces for each tag
    for tag in tag_totals.index:
        tag_data = daily_spending[daily_spending['tag'] == tag].copy()
        
        # Convert day of year to theta (degrees: 0-360)
        tag_data['theta'] = (tag_data['day_of_year'] - 1) * 360 / 365
        
        fig.add_trace(go.Scatterpolar(
            r=tag_data['amount'],
            theta=tag_data['theta'],
            mode='markers',
            name=tag,
            marker=dict(
                size=8,
                color=colors.get(tag, 'gray'),
                line=dict(width=0.5, color='white')
            ),
            customdata=tag_data[['date_str', 'amount']],
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Date: %{customdata[0]}<br>' +
                         'Amount: €%{customdata[1]:.2f}<br>' +
                         '<extra></extra>',
        ))
    
    # Add a trace for total daily spending (larger markers, black)
    daily_totals['theta'] = (daily_totals['day_of_year'] - 1) * 360 / 365
    fig.add_trace(go.Scatterpolar(
        r=daily_totals['amount'],
        theta=daily_totals['theta'],
        mode='markers',
        name='Total Daily',
        marker=dict(
            size=6,
            color='rgba(0,0,0,0.4)',
            symbol='circle',
            line=dict(width=0)
        ),
        customdata=daily_totals[['date_str', 'amount']],
        hovertemplate='<b>Total Daily Spending</b><br>' +
                     'Date: %{customdata[0]}<br>' +
                     'Amount: €%{customdata[1]:.2f}<br>' +
                     '<extra></extra>',
    ))
    
    # Update layout for polar chart
    fig.update_layout(
        title=f'Daily Spending Pattern - Year {year} (Circular View)',
        polar=dict(
            radialaxis=dict(
                title='Spending (EUR)',
                visible=True,
                showline=True,
                showticklabels=True,
            ),
            angularaxis=dict(
                direction='clockwise',
                period=360,
                rotation=90,  # Start at top (January 1st)
                tickmode='array',
                tickvals=[0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330],
                ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                         'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            )
        ),
        showlegend=True,
        legend=dict(
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02,
        ),
        width=1000,
        height=900,
    )
    
    # Save to HTML
    output_file = f'daily_spending_circle_{year}.html'
    save_and_open_html(fig, output_file, chart_label='Circular daily spending visualization')
    print(f"Showing data for year {year} with {len(daily_totals)} days of transactions")


# Function to create pie chart showing daily spending rate per tag

def visualize_daily_rate_pie(df, year=None):
    """
    Creates a pie chart showing percentage of spending per tag,
    with daily average rates displayed.
    Daily rate is calculated by dividing total spending by 365 if there are 12 months of data.
    
    Args:
        df: DataFrame with financial data
        year: Single year (int), list of years, or None (uses most recent year)
    """
    # Handle list of years - create combined chart first, then individual charts
    if isinstance(year, list):
        # Create combined average chart for all years
        print(f"\nCreating combined average chart for years: {year}")
        df_exp = df[df['value'] < 0].copy()
        if not df_exp.empty:
            # Filter for all selected years
            df_combined = df_exp[df_exp['time'].dt.year.isin(year)].copy()
            
            if not df_combined.empty:
                # Calculate total days covered (unique days across all years)
                total_unique_days = 0
                for y in year:
                    year_data = df_combined[df_combined['time'].dt.year == y]
                    if not year_data.empty:
                        months_in_year = year_data['time'].dt.to_period('M').nunique()
                        if months_in_year == 12:
                            total_unique_days += 365
                        else:
                            total_unique_days += year_data['time'].dt.dayofyear.nunique()
                
                # Flip sign so expenses are positive
                df_combined['amount'] = -df_combined['value']
                
                # Sum by tag across all years
                tag_totals = df_combined.groupby('tag')['amount'].sum().sort_values(ascending=False)
                
                # Calculate daily rates averaged across all selected years
                daily_rates = tag_totals / total_unique_days
                
                # Calculate percentages
                total_spending = tag_totals.sum()
                percentages = (tag_totals / total_spending * 100)
                
                # Get consistent color mapping
                color_map = get_tag_color_map(tag_totals.index)
                colors = [color_map[tag] for tag in tag_totals.index]
                
                # Create custom text for each slice
                labels = []
                hover_texts = []
                for tag in tag_totals.index:
                    labels.append(f"{tag}")
                    hover_texts.append(
                        f"<b>{tag}</b><br>" +
                        f"Total: €{tag_totals[tag]:.2f}<br>" +
                        f"Daily avg: €{daily_rates[tag]:.2f}<br>" +
                        f"Percentage: {percentages[tag]:.1f}%"
                    )
                
                # Create pie chart
                fig = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=tag_totals.values,
                    text=[f"€{rate:.2f}/day" for rate in daily_rates],
                    textposition='inside',
                    textfont=dict(size=11, color='white'),
                    hovertext=hover_texts,
                    hoverinfo='text',
                    marker=dict(
                        colors=colors,
                        line=dict(color='white', width=2)
                    ),
                    pull=[0.05 if i == 0 else 0 for i in range(len(tag_totals))]
                )])
                
                # Update layout
                years_str = ', '.join(map(str, year))
                fig.update_layout(
                    title=f'Daily Spending Rate by Tag - Combined Average<br><sub>Years: {years_str} | Total: €{total_spending:.2f} | {total_unique_days} days</sub>',
                    width=1000,
                    height=800,
                    showlegend=True,
                    legend=dict(
                        orientation='v',
                        yanchor='middle',
                        y=0.5,
                        xanchor='left',
                        x=1.02,
                    )
                )
                
                # Save to HTML
                output_file = f'daily_rate_pie_combined_{"_".join(map(str, year))}.html'
                save_and_open_html(fig, output_file, chart_label='Combined daily rate pie chart')
                print(f"Total across {len(year)} years: €{total_spending:.2f} over {total_unique_days} days")
        
        # Now create individual charts for each year
        for y in year:
            visualize_daily_rate_pie(df, year=y)
        return
    
    # Filter for expenses only (negative values)
    df_exp = df[df['value'] < 0].copy()
    if df_exp.empty:
        print("No expenses found for daily rate pie chart.")
        return
    
    # Determine which year to visualize
    if year is None:
        year = df_exp['time'].dt.year.max()
    
    # Filter for the selected year
    df_year = df_exp[df_exp['time'].dt.year == year].copy()
    
    if df_year.empty:
        print(f"No expenses found for year {year}.")
        return
    
    # Check how many months have data
    unique_months = df_year['time'].dt.to_period('M').nunique()
    
    # Determine divisor: use 365 if we have 12 months, otherwise use actual days
    if unique_months == 12:
        days_divisor = 365
        divisor_note = "12 months"
    else:
        days_divisor = df_year['time'].dt.dayofyear.nunique()
        divisor_note = f"{unique_months} months ({days_divisor} days)"
    
    # Flip sign so expenses are positive
    df_year['amount'] = -df_year['value']
    
    # Sum by tag
    tag_totals = df_year.groupby('tag')['amount'].sum().sort_values(ascending=False)
    
    # Calculate daily rates
    daily_rates = tag_totals / days_divisor
    
    # Calculate percentages
    total_spending = tag_totals.sum()
    percentages = (tag_totals / total_spending * 100)
    
    # Get consistent color mapping
    color_map = get_tag_color_map(tag_totals.index)
    colors = [color_map[tag] for tag in tag_totals.index]
    
    # Create custom text for each slice
    labels = []
    hover_texts = []
    for tag in tag_totals.index:
        labels.append(f"{tag}")
        hover_texts.append(
            f"<b>{tag}</b><br>" +
            f"Total: €{tag_totals[tag]:.2f}<br>" +
            f"Daily avg: €{daily_rates[tag]:.2f}<br>" +
            f"Percentage: {percentages[tag]:.1f}%"
        )
    
    # Create pie chart
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=tag_totals.values,
        text=[f"€{rate:.2f}/day" for rate in daily_rates],
        textposition='inside',
        textfont=dict(size=11, color='white'),
        hovertext=hover_texts,
        hoverinfo='text',
        marker=dict(
            colors=colors,
            line=dict(color='white', width=2)
        ),
        pull=[0.05 if i == 0 else 0 for i in range(len(tag_totals))]  # Pull out largest slice
    )])
    
    # Update layout
    fig.update_layout(
        title=f'Daily Spending Rate by Tag - Year {year}<br><sub>Based on {divisor_note}, Total: €{total_spending:.2f}</sub>',
        width=1000,
        height=800,
        showlegend=True,
        legend=dict(
            orientation='v',
            yanchor='middle',
            y=0.5,
            xanchor='left',
            x=1.02,
        )
    )
    
    # Save to HTML
    output_file = f'daily_rate_pie_{year}.html'
    save_and_open_html(fig, output_file, chart_label='Daily rate pie chart')
    print(f"Year {year}: {divisor_note}, dividing by {days_divisor} days")
    print(f"Total spending: €{total_spending:.2f}")


# Function to check for anomalies and automated transactions

def check_anomaly(df):
    """
    Evaluates transactions per tag for automated/repeated patterns.
    For each transaction (starting from 2nd), compares with previous transaction(s)
    to determine if it's part of an automated/repeated pattern.
    
    Returns a dataframe with a repetition_score (0-1) for each transaction:
    - 0: no repeated pattern (values differ significantly or intervals vary)
    - 1: perfect repeated transaction (identical value, identical time interval as previous)
    - 0-1: partial repetition (some consistency in values and/or intervals)
    
    Args:
        df: DataFrame with financial data (must include 'tag', 'time', and 'value' columns)
    
    Returns:
        Original DataFrame + columns: days_since_prev, repetition_score
    """
    result_df = df.copy()
    result_df['days_since_prev'] = pd.NA
    result_df['repetition_score'] = 0.0

    for tag in result_df['tag'].dropna().unique():
        tag_idx = result_df[result_df['tag'] == tag].sort_values('time').index.tolist()

        if len(tag_idx) < 2:
            continue

        for pos in range(1, len(tag_idx)):
            curr_idx = tag_idx[pos]
            prev_idx = tag_idx[pos - 1]

            current = result_df.loc[curr_idx]
            previous = result_df.loc[prev_idx]

            value_diff = abs(current['value'] - previous['value'])
            current_interval = (current['time'] - previous['time']).days

            max_val = max(abs(current['value']), abs(previous['value']))
            if max_val == 0:
                value_score = 1.0 if value_diff == 0 else 0.0
            else:
                value_score = max(0.0, 1.0 - (value_diff / max_val))

            if pos >= 2:
                prev_prev_idx = tag_idx[pos - 2]
                prev_prev = result_df.loc[prev_prev_idx]
                previous_interval = (previous['time'] - prev_prev['time']).days

                interval_diff = abs(current_interval - previous_interval)
                if previous_interval == 0:
                    interval_score = 1.0 if current_interval == 0 else 0.0
                else:
                    interval_score = max(0.0, 1.0 - (interval_diff / max(abs(previous_interval), 1)))
            else:
                interval_score = 0

            repetition_score = (value_score * interval_score)

            result_df.loc[curr_idx, 'days_since_prev'] = current_interval
            result_df.loc[curr_idx, 'repetition_score'] = round(repetition_score, 3)

    return result_df


def visualize_prediction_index_over_time(anomaly_df):
    """
    Create a monthly time-series per tag using the metric:

        prediction_index = 30 / avg(days_since_prev) * avg(repetition_score) * avg(value)

    computed per (month, tag) using the anomaly-enriched dataframe.
    """
    df = anomaly_df.copy()

    # Only use rows where we have a valid interval
    mask = df['days_since_prev'].notna() & (df['days_since_prev'] > 0)
    df = df.loc[mask].copy()

    if df.empty:
        print("No data with valid days_since_prev to build prediction index over time.")
        return

    # Group by calendar month (month-end) and tag
    monthly = (
        df
        .groupby([pd.Grouper(key='time', freq='ME'), 'tag'])
        .agg(
            avg_days_since_prev=('days_since_prev', 'mean'),
            avg_repetition_score=('repetition_score', 'mean'),
            avg_value=('value', 'mean'),
        )
        .reset_index()
    )

    if monthly.empty:
        print("No monthly groups available for prediction index.")
        return

    # Compute the metric per (month, tag)
    monthly['prediction_index'] = (
        30.0 / monthly['avg_days_since_prev'] *
        monthly['avg_repetition_score'] *
        monthly['avg_value']
    )

    # Export the clean monthly dataframe for further analysis
    monthly.to_csv('monthly_prediction_index.csv', sep=';', index=False, encoding='utf-8')
    print("Exported monthly prediction index to monthly_prediction_index.csv")

    # Prepare Plotly line chart: one line per tag across months
    fig = go.Figure()

    # Ensure time is sorted
    monthly = monthly.sort_values('time')

    # Stable tag order based on total absolute prediction index
    tag_order = (
        monthly
        .groupby('tag')['prediction_index']
        .apply(lambda s: s.abs().sum())
        .sort_values(ascending=False)
        .index
        .tolist()
    )

    colors = get_tag_color_map(tag_order)

    for tag in tag_order:
        tag_data = monthly[monthly['tag'] == tag]
        fig.add_trace(
            go.Scatter(
                name=tag,
                x=tag_data['time'],
                y=tag_data['prediction_index'],
                mode='lines+markers',
                line=dict(color=colors.get(tag, 'gray')),
                marker=dict(color=colors.get(tag, 'gray')),
                hovertemplate='<b>%{fullData.name}</b><br>' +
                             'Month: %{x|%Y-%m}<br>' +
                             'Index: %{y:.2f}<br>' +
                             '<extra></extra>',
            )
        )

    # Add a total monthly prediction index line (sum over all tags per month)
    total_monthly = (
        monthly
        .groupby('time')['prediction_index']
        .sum()
        .reset_index(name='total_prediction_index')
        .sort_values('time')
    )

    fig.add_trace(
        go.Scatter(
            name='Total',
            x=total_monthly['time'],
            y=total_monthly['total_prediction_index'],
            mode='lines+markers',
            line=dict(color='black', width=3),
            marker=dict(color='black', size=7),
            hovertemplate='<b>Total</b><br>' +
                         'Month: %{x|%Y-%m}<br>' +
                         'Total index: %{y:.2f}<br>' +
                         '<extra></extra>',
        )
    )

    fig.update_layout(
        title='Monthly Prediction Index by Tag',
        xaxis_title='Month',
        yaxis_title='Prediction index (30 / avg_days * avg_rep * avg_value)',
        hovermode='x unified',
        autosize=True,
        xaxis=dict(autorange=True),
        yaxis=dict(autorange=True),
        legend=dict(
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02,
        )
    )

    output_file = 'monthly_prediction_index.html'
    save_and_open_html(
        fig,
        output_file,
        chart_label='Monthly prediction index over time',
    )


# Main function to execute the pipeline

def main():
    directory = 'CSV_folder'  # Change this to your CSV folder path
    tags_file = 'unique_ids_tags.csv'
    
    # Load and process data
    dataframes = load_csv_files(directory)
    combined_df = create_dataframe(dataframes)
    processed_df = process_dataframe(combined_df)
    
    # Write/update unique IDs with tags
    write_unique_ids(processed_df, tags_file)
    
    # Apply tags to dataframe (empty tags become 'other')
    tagged_df = apply_tags_to_dataframe(processed_df, tags_file)
    check_anomaly_df = check_anomaly(tagged_df)

    # Export dataframe to CSV
    output_csv = 'processed_finances.csv'
    tagged_df.to_csv(output_csv, sep=';', index=False, encoding='utf-8')
    check_anomaly_df.to_csv('processed_finances_with_anomalies.csv', sep=';', index=False, encoding='utf-8')
    print(f"\nExported processed dataframe to {output_csv}")

    anomaly_csv = 'anomaly_scores.csv'
    check_anomaly_df[['time', 'tag', 'id', 'value', 'days_since_prev', 'repetition_score']].to_csv(
        anomaly_csv,
        sep=';',
        index=False,
        encoding='utf-8'
    )
    print(f"Exported anomaly dataframe to {anomaly_csv}")

    # Visualization: monthly prediction index per tag over time
    visualize_prediction_index_over_time(check_anomaly_df)
    
    # Visualize finances (waterfall-style stacked bars)
    # To add a cumulative line for a specific tag, pass accumulate_tag="your_tag_name".
    # Example: visualize_finances_interactive(tagged_df, accumulate_tag="huur")
    visualize_finances_interactive(tagged_df, accumulate_tag="school")

    # Additional small line chart: monthly expenses per tag
    visualize_monthly_expenses_line(tagged_df)
    
    # Pie chart: daily spending rate by tag (supports list of years)
    visualize_daily_rate_pie(tagged_df, year=[2022, 2023, 2024, 2025])

if __name__ == '__main__':
    main()