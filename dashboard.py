#!/usr/bin/env python3
"""
Streamlit Dashboard for Option Chain Data
Real-time visualization of data stored in PostgreSQL
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from database import engine
from sqlalchemy import text
from datetime import datetime, timedelta
import time
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Option Chain Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .data-table {
        background-color: white;
        border-radius: 0.5rem;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .chart-container {
        background-color: white;
        border-radius: 0.5rem;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .stAlert {
        border-radius: 0.5rem;
    }
    .stButton > button {
        border-radius: 0.5rem;
        font-weight: bold;
    }
    .stSelectbox > div > div {
        border-radius: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_available_symbols():
    """Get list of available symbols from database"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name LIKE 'option_chain%'
                ORDER BY schema_name
            """))
            schemas = [row[0] for row in result]
            symbols = [schema.replace('option_chain_', '').upper() for schema in schemas if schema != 'option_chain']
            return symbols
    except Exception as e:
        st.error(f"Error getting symbols: {str(e)}")
        return []

@st.cache_data(ttl=60)
def get_available_expiries(symbol):
    """Get list of available expiries for a symbol"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'option_chain_{symbol.lower()}'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
            expiries = []
            for table in tables:
                expiry_part = table.replace(f"{symbol.lower()}_", "")
                expiry_date = expiry_part.replace("_", " ")
                expiries.append(expiry_date)
            return expiries
    except Exception as e:
        st.error(f"Error getting expiries: {str(e)}")
        return []

@st.cache_data(ttl=60)
def get_available_dates(symbol, expiry_date=None):
    """Get list of available trading dates from database"""
    try:
        if expiry_date:
            table_name = f"{symbol}_{expiry_date.replace(' ', '_').replace('-', '_')}"
            query = f"""
            SELECT DISTINCT DATE(fetch_time) as trading_date
            FROM option_chain_{symbol.lower()}.{table_name}
            ORDER BY trading_date DESC
            """
        else:
            # Get data from all tables for the symbol
            query = f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'option_chain_{symbol.lower()}'
            ORDER BY table_name;
            """
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                tables = [row[0] for row in result]
            
            if not tables:
                return []
            
            # Union all tables to get dates
            union_queries = []
            for table in tables:
                union_queries.append(f"SELECT DISTINCT DATE(fetch_time) as trading_date FROM option_chain_{symbol.lower()}.{table}")
            
            query = " UNION ".join(union_queries) + " ORDER BY trading_date DESC"
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            dates = [row[0] for row in result]
            return dates
    except Exception as e:
        st.error(f"Error getting dates: {str(e)}")
        return []

@st.cache_data(ttl=30)  # Cache for 30 seconds for real-time updates
def get_data_by_timeframe(symbol, expiry_date=None, selected_date=None, timeframe_minutes=1):
    """Get data filtered by date and time interval"""
    try:
        if expiry_date:
            table_name = f"{symbol}_{expiry_date.replace(' ', '_').replace('-', '_')}"
            query = f"""
            SELECT * FROM option_chain_{symbol.lower()}.{table_name}
            WHERE DATE(fetch_time) = '{selected_date}'
            ORDER BY fetch_time ASC, timestamp ASC
            """
        else:
            # Get data from all tables for the symbol
            query = f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'option_chain_{symbol.lower()}'
            ORDER BY table_name;
            """
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                tables = [row[0] for row in result]
            
            if not tables:
                return pd.DataFrame()
            
            # Union all tables
            union_queries = []
            for table in tables:
                union_queries.append(f"SELECT * FROM option_chain_{symbol.lower()}.{table}")
            
            query = " UNION ALL ".join(union_queries) + f" WHERE DATE(fetch_time) = '{selected_date}' ORDER BY fetch_time ASC, timestamp ASC"
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        if not df.empty:
            # Convert fetch_time to datetime if it's not already
            df['fetch_time'] = pd.to_datetime(df['fetch_time'])
            
            # Apply time interval filtering
            if timeframe_minutes > 1:
                # Round timestamps UP to the nearest interval
                df['time_rounded'] = df['fetch_time'].dt.ceil(f'{timeframe_minutes}T')
                
                # For each interval and strike, get the last available data point
                df = df.groupby(['time_rounded', 'Strike Price']).last().reset_index()
                
                # Set the fetch_time to the interval time for consistency
                df['fetch_time'] = df['time_rounded']
                df = df.drop(columns=['time_rounded'])
        
        return df.sort_values(['fetch_time', 'Strike Price']).reset_index(drop=True)
    except Exception as e:
        st.error(f"Error getting data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_dashboard_stats():
    """Get overall dashboard statistics"""
    try:
        with engine.connect() as conn:
            # Total records
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT table_schema) as total_symbols,
                    COUNT(DISTINCT table_name) as total_tables
                FROM information_schema.tables 
                WHERE table_schema LIKE 'option_chain%'
            """))
            stats = result.fetchone()
            
            # Latest data time
            result = conn.execute(text("""
                SELECT MAX(fetch_time) as latest_time
                FROM (
                    SELECT fetch_time FROM option_chain_nifty.nifty_26_jun
                    UNION ALL
                    SELECT fetch_time FROM option_chain_banknifty.banknifty_26_jun
                    LIMIT 1000
                ) as latest
            """))
            latest_time = result.fetchone()
            
            return {
                'total_records': stats[0] if stats[0] else 0,
                'total_symbols': stats[1] if stats[1] else 0,
                'total_tables': stats[2] if stats[2] else 0,
                'latest_time': latest_time[0] if latest_time and latest_time[0] else None
            }
    except Exception as e:
        return {
            'total_records': 0,
            'total_symbols': 0,
            'total_tables': 0,
            'latest_time': None
        }

def create_spot_price_chart(df):
    """Create spot price trend chart"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Group by fetch_time and get latest spot price
    spot_data = df.groupby('fetch_time')['Spot Price'].last().reset_index()
    
    fig.add_trace(go.Scatter(
        x=spot_data['fetch_time'],
        y=spot_data['Spot Price'],
        mode='lines+markers',
        name='Spot Price',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8, color='#1f77b4'),
        fill='tonexty',
        fillcolor='rgba(31, 119, 180, 0.1)'
    ))
    
    fig.update_layout(
        title='📈 Spot Price Trend Over Time',
        xaxis_title='Time',
        yaxis_title='Spot Price',
        height=400,
        showlegend=True,
        template='plotly_white',
        hovermode='x unified'
    )
    
    return fig

def create_oi_chart(df):
    """Create OI comparison chart"""
    if df.empty:
        return go.Figure()
    
    # Get latest data for each strike
    latest_data = df.groupby('Strike Price').last().reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=latest_data['Strike Price'],
        y=latest_data['CE OI'],
        name='CE OI',
        marker_color='#2ca02c',
        opacity=0.8
    ))
    
    fig.add_trace(go.Bar(
        x=latest_data['Strike Price'],
        y=latest_data['PE OI'],
        name='PE OI',
        marker_color='#d62728',
        opacity=0.8
    ))
    
    fig.update_layout(
        title='📊 Open Interest by Strike Price',
        xaxis_title='Strike Price',
        yaxis_title='Open Interest',
        height=400,
        barmode='group',
        template='plotly_white',
        showlegend=True
    )
    
    return fig

def create_volume_chart(df):
    """Create volume comparison chart"""
    if df.empty:
        return go.Figure()
    
    # Get latest data for each strike
    latest_data = df.groupby('Strike Price').last().reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=latest_data['Strike Price'],
        y=latest_data['CE Volume'],
        name='CE Volume',
        marker_color='#17a2b8',
        opacity=0.8
    ))
    
    fig.add_trace(go.Bar(
        x=latest_data['Strike Price'],
        y=latest_data['PE Volume'],
        name='PE Volume',
        marker_color='#fd7e14',
        opacity=0.8
    ))
    
    fig.update_layout(
        title='📈 Volume by Strike Price',
        xaxis_title='Strike Price',
        yaxis_title='Volume',
        height=400,
        barmode='group',
        template='plotly_white',
        showlegend=True
    )
    
    return fig

def create_iv_chart(df):
    """Create Implied Volatility chart"""
    if df.empty:
        return go.Figure()
    
    # Get latest data for each strike
    latest_data = df.groupby('Strike Price').last().reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=latest_data['Strike Price'],
        y=latest_data['CE IV'],
        mode='lines+markers',
        name='CE IV',
        line=dict(color='#2ca02c', width=3),
        marker=dict(size=6)
    ))
    
    fig.add_trace(go.Scatter(
        x=latest_data['Strike Price'],
        y=latest_data['PE IV'],
        mode='lines+markers',
        name='PE IV',
        line=dict(color='#d62728', width=3),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title='📊 Implied Volatility by Strike Price',
        xaxis_title='Strike Price',
        yaxis_title='Implied Volatility (%)',
        height=400,
        template='plotly_white',
        showlegend=True,
        hovermode='x unified'
    )
    
    return fig

def create_greeks_chart(df, greek_type='Delta'):
    """Create Greeks comparison chart"""
    if df.empty:
        return go.Figure()
    
    # Get latest data for each strike
    latest_data = df.groupby('Strike Price').last().reset_index()
    
    fig = go.Figure()
    
    ce_greek_col = f'CE {greek_type}'
    pe_greek_col = f'PE {greek_type}'
    
    if ce_greek_col in latest_data.columns and pe_greek_col in latest_data.columns:
        fig.add_trace(go.Scatter(
            x=latest_data['Strike Price'],
            y=latest_data[ce_greek_col],
            mode='lines+markers',
            name=f'CE {greek_type}',
            line=dict(color='#2ca02c', width=3),
            marker=dict(size=6)
        ))
        
        fig.add_trace(go.Scatter(
            x=latest_data['Strike Price'],
            y=latest_data[pe_greek_col],
            mode='lines+markers',
            name=f'PE {greek_type}',
            line=dict(color='#d62728', width=3),
            marker=dict(size=6)
        ))
    
    fig.update_layout(
        title=f'📊 {greek_type} by Strike Price',
        xaxis_title='Strike Price',
        yaxis_title=f'{greek_type}',
        height=400,
        template='plotly_white',
        showlegend=True,
        hovermode='x unified'
    )
    
    return fig

def create_pcr_chart(df):
    """Create Put-Call Ratio chart"""
    if df.empty:
        return go.Figure()
    
    # Group by fetch_time and calculate PCR
    pcr_data = df.groupby('fetch_time').agg({
        'PE OI': 'sum',
        'CE OI': 'sum'
    }).reset_index()
    
    pcr_data['PCR'] = pcr_data['PE OI'] / pcr_data['CE OI']
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=pcr_data['fetch_time'],
        y=pcr_data['PCR'],
        mode='lines+markers',
        name='Put-Call Ratio',
        line=dict(color='#9467bd', width=3),
        marker=dict(size=8)
    ))
    
    # Add horizontal line at PCR = 1
    fig.add_hline(y=1, line_dash="dash", line_color="red", 
                  annotation_text="PCR = 1 (Neutral)")
    
    fig.update_layout(
        title='📊 Put-Call Ratio Over Time',
        xaxis_title='Time',
        yaxis_title='Put-Call Ratio',
        height=400,
        template='plotly_white',
        showlegend=True,
        hovermode='x unified'
    )
    
    return fig

def create_heatmap(df):
    """Create heatmap of OI data"""
    if df.empty:
        return go.Figure()
    
    # Get latest data for each strike
    latest_data = df.groupby('Strike Price').last().reset_index()
    
    # Create heatmap data
    strikes = latest_data['Strike Price'].tolist()
    ce_oi = latest_data['CE OI'].tolist()
    pe_oi = latest_data['PE OI'].tolist()
    
    fig = go.Figure(data=go.Heatmap(
        z=[ce_oi, pe_oi],
        x=strikes,
        y=['CE OI', 'PE OI'],
        colorscale='Viridis',
        showscale=True
    ))
    
    fig.update_layout(
        title='🔥 Open Interest Heatmap',
        xaxis_title='Strike Price',
        yaxis_title='Option Type',
        height=400,
        template='plotly_white'
    )
    
    return fig

def create_stacked_bar_chart(pivot_df, title=""):
    """Create a stacked bar chart from the pivot table data."""
    if pivot_df.empty:
        return go.Figure()

    fig = go.Figure()
    
    # The first two columns are 'Instrument' and 'Strike Price', the rest are time intervals
    time_cols = pivot_df.columns[2:]
    
    for time_col in time_cols:
        # Format the text to be displayed on the bars
        bar_text = pivot_df[time_col].apply(lambda x: f'{x:,.0f}' if x > 0 else '')
        
        fig.add_trace(go.Bar(
            x=pivot_df['Strike Price'],
            y=pivot_df[time_col],
            name=time_col,
            text=bar_text,
            textposition='inside'
        ))

    fig.update_layout(
        barmode='stack',
        title_text=f'<b>{title} Distribution Over Time</b>',
        xaxis_title='Strike Price',
        yaxis_title=f'Total {title}',
        legend_title='Time Interval',
        height=500,
        template='plotly_white'
    )
    
    fig.update_traces(textangle=0)
    
    return fig

def create_pivot_table(df, value_col='CE OI'):
    """Create a pivot table of a selected metric over time for each strike."""
    if df.empty or value_col not in df.columns:
        st.warning("Not enough data to create a pivot table for the selected timeframe.")
        return pd.DataFrame()

    try:
        df['fetch_time'] = pd.to_datetime(df['fetch_time'])
        df['time_str'] = df['fetch_time'].dt.strftime('%H:%M')
        
        pivot_df = df.pivot_table(
            index='Strike Price', 
            columns='time_str', 
            values=value_col,
            aggfunc='last'
        )
        
        pivot_df = pivot_df.reset_index()

        if 'Symbol' in df.columns:
            symbol = df['Symbol'].iloc[0]
            pivot_df.insert(0, 'Instrument', symbol)
        
        pivot_df = pivot_df.fillna(0)
        return pivot_df
    except Exception as e:
        st.error(f"Error creating pivot table: {e}")
        return pd.DataFrame()

def calculate_analytics(df):
    """Calculate various analytics from the data"""
    if df.empty:
        return {}
    
    analytics = {}
    
    # Latest data
    latest = df.groupby('Strike Price').last().reset_index()
    
    # Basic stats
    analytics['total_strikes'] = len(latest)
    analytics['avg_spot_price'] = latest['Spot Price'].mean()
    analytics['current_spot'] = latest['Spot Price'].iloc[0] if len(latest) > 0 else 0
    
    # OI Analysis
    analytics['total_ce_oi'] = latest['CE OI'].sum()
    analytics['total_pe_oi'] = latest['PE OI'].sum()
    analytics['pcr'] = analytics['total_pe_oi'] / analytics['total_ce_oi'] if analytics['total_ce_oi'] > 0 else 0
    
    # Volume Analysis
    analytics['total_ce_volume'] = latest['CE Volume'].sum()
    analytics['total_pe_volume'] = latest['PE Volume'].sum()
    
    # IV Analysis
    analytics['avg_ce_iv'] = latest['CE IV'].mean()
    analytics['avg_pe_iv'] = latest['PE IV'].mean()
    
    # Find ATM strike
    if 'ATM Strike' in latest.columns:
        atm_strike = latest['ATM Strike'].iloc[0]
        atm_data = latest[latest['Strike Price'] == atm_strike]
        if not atm_data.empty:
            analytics['atm_ce_oi'] = atm_data['CE OI'].iloc[0]
            analytics['atm_pe_oi'] = atm_data['PE OI'].iloc[0]
            analytics['atm_ce_iv'] = atm_data['CE IV'].iloc[0]
            analytics['atm_pe_iv'] = atm_data['PE IV'].iloc[0]
    
    return analytics

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Option Chain Dashboard</h1>', unsafe_allow_html=True)
    
    # Auto-refresh
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button('🔄 Refresh Data', use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Sidebar
    st.sidebar.title("📋 Dashboard Controls")
    
    # Get available symbols
    symbols = get_available_symbols()
    
    if not symbols:
        st.warning("No data available. Please start the data collection first.")
        return
    
    # Symbol selection
    selected_symbol = st.sidebar.selectbox(
        "Select Symbol:",
        symbols,
        index=0 if symbols else None
    )
    
    # Expiry selection
    if selected_symbol:
        expiries = get_available_expiries(selected_symbol)
        selected_expiry = st.sidebar.selectbox(
            "Select Expiry:",
            ["All Expiries"] + expiries,
            index=0
        )
    
    # Date selection
    if selected_symbol:
        available_dates = get_available_dates(selected_symbol, None if selected_expiry == "All Expiries" else selected_expiry)
        if available_dates:
            selected_date = st.sidebar.selectbox(
                "Select Trading Date:",
                available_dates,
                index=0
            )
        else:
            st.sidebar.warning("No trading dates available")
            return
    
    # Time interval selection
    timeframe_options = {
        "1 Minute": 1,
        "5 Minutes": 5,
        "15 Minutes": 15,
        "30 Minutes": 30,
        "1 Hour": 60
    }
    
    selected_timeframe = st.sidebar.selectbox(
        "Select Time Interval:",
        list(timeframe_options.keys()),
        index=0
    )
    
    timeframe_minutes = timeframe_options[selected_timeframe]
    
    # Chart selection
    st.sidebar.title("📊 Chart Options")
    show_spot_chart = st.sidebar.checkbox("Spot Price Trend", value=True)
    show_oi_chart = st.sidebar.checkbox("Open Interest", value=True)
    show_volume_chart = st.sidebar.checkbox("Volume Analysis", value=True)
    show_iv_chart = st.sidebar.checkbox("Implied Volatility", value=True)
    show_greeks_chart = st.sidebar.checkbox("Greeks Analysis", value=True)
    show_pcr_chart = st.sidebar.checkbox("Put-Call Ratio", value=True)
    show_heatmap = st.sidebar.checkbox("OI Heatmap", value=True)
    
    # Greeks type selection
    if show_greeks_chart:
        greek_type = st.sidebar.selectbox(
            "Select Greek:",
            ["Delta", "Gamma", "Theta", "Vega"],
            index=0
        )
    
    # Main content
    col1, col2, col3, col4 = st.columns(4)
    
    # Dashboard statistics
    stats = get_dashboard_stats()
    
    with col1:
        st.metric("📈 Total Records", f"{stats['total_records']:,}")
    
    with col2:
        st.metric("📊 Total Symbols", stats['total_symbols'])
    
    with col3:
        st.metric("📋 Total Tables", stats['total_tables'])
    
    with col4:
        if stats['latest_time']:
            st.metric("🕐 Latest Update", stats['latest_time'].strftime('%H:%M:%S'))
        else:
            st.metric("🕐 Latest Update", "No data")
    
    # Get data
    if selected_symbol and 'selected_date' in locals():
        expiry_filter = None if selected_expiry == "All Expiries" else selected_expiry
        df = get_data_by_timeframe(selected_symbol, expiry_filter, selected_date, timeframe_minutes)
        
        if not df.empty:
            # Calculate analytics
            analytics = calculate_analytics(df)
            
            # Analytics section
            st.subheader(f"📊 Market Analytics - {selected_date} ({selected_timeframe})")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Current Spot", f"{analytics.get('current_spot', 0):.2f}")
                st.metric("Total CE OI", f"{analytics.get('total_ce_oi', 0):,}")
            
            with col2:
                st.metric("Put-Call Ratio", f"{analytics.get('pcr', 0):.2f}")
                st.metric("Total PE OI", f"{analytics.get('total_pe_oi', 0):,}")
            
            with col3:
                st.metric("Avg CE IV", f"{analytics.get('avg_ce_iv', 0):.2f}%")
                st.metric("Total CE Volume", f"{analytics.get('total_ce_volume', 0):,}")
            
            with col4:
                st.metric("Avg PE IV", f"{analytics.get('avg_pe_iv', 0):.2f}%")
                st.metric("Total PE Volume", f"{analytics.get('total_pe_volume', 0):,}")
            
            # Charts section
            st.subheader("📈 Market Charts")
            
            # First row of charts
            if show_spot_chart or show_oi_chart:
                col1, col2 = st.columns(2)
                
                with col1:
                    if show_spot_chart:
                        spot_chart = create_spot_price_chart(df)
                        st.plotly_chart(spot_chart, use_container_width=True)
                
                with col2:
                    if show_oi_chart:
                        oi_chart = create_oi_chart(df)
                        st.plotly_chart(oi_chart, use_container_width=True)
            
            # Second row of charts
            if show_volume_chart or show_iv_chart:
                col1, col2 = st.columns(2)
                
                with col1:
                    if show_volume_chart:
                        volume_chart = create_volume_chart(df)
                        st.plotly_chart(volume_chart, use_container_width=True)
                
                with col2:
                    if show_iv_chart:
                        iv_chart = create_iv_chart(df)
                        st.plotly_chart(iv_chart, use_container_width=True)
            
            # Third row of charts
            if show_greeks_chart or show_pcr_chart:
                col1, col2 = st.columns(2)
                
                with col1:
                    if show_greeks_chart:
                        greeks_chart = create_greeks_chart(df, greek_type)
                        st.plotly_chart(greeks_chart, use_container_width=True)
                
                with col2:
                    if show_pcr_chart:
                        pcr_chart = create_pcr_chart(df)
                        st.plotly_chart(pcr_chart, use_container_width=True)
            
            # Fourth row - heatmap
            if show_heatmap:
                heatmap_chart = create_heatmap(df)
                st.plotly_chart(heatmap_chart, use_container_width=True)
            
            # Pivot Table Section
            st.subheader("📈 Strike vs. Time Analysis")
            
            col1, col2 = st.columns([1, 3])
            with col1:
                pivot_metric = st.selectbox(
                    "Select Metric to Analyze:",
                    ['CE OI', 'PE OI', 'CE Volume', 'PE Volume', 'CE Chg in OI', 'PE Chg in OI', 'CE IV', 'PE IV', 'CE LTP', 'PE LTP'],
                    key='pivot_metric'
                )

            if pivot_metric:
                pivot_table_df = create_pivot_table(df, value_col=pivot_metric)

                if not pivot_table_df.empty:
                    # Display the new stacked bar chart
                    st.markdown(f"### Stacked Bar Chart: {pivot_metric}")
                    stacked_chart = create_stacked_bar_chart(pivot_table_df, title=pivot_metric)
                    st.plotly_chart(stacked_chart, use_container_width=True)
                    
                    # Display the data table
                    st.markdown(f"### Data Table: {pivot_metric}")
                    st.markdown(f"Displaying **{pivot_metric}** for each strike at different time intervals.")
                    
                    # Determine formatting for the values in the pivot table
                    if 'OI' in pivot_metric or 'Volume' in pivot_metric:
                        format_str = "{:,.0f}"
                    else:
                        format_str = "{:,.2f}"
                    
                    # Build a format dictionary for all numeric columns except the first two
                    format_dict = {col: format_str for col in pivot_table_df.columns if col not in ['Instrument', 'Strike Price']}
                    styled_df = pivot_table_df.style.format(format_dict)
                    
                    st.dataframe(styled_df, use_container_width=True)
            
            # Data tables section
            st.subheader("📋 Raw Data Tables")
            
            # Latest data table
            st.markdown('<div class="data-table">', unsafe_allow_html=True)
            st.subheader("📋 Latest Data")
            
            # Show latest records
            latest_records = df.head(10)[['Symbol', 'expiry_date', 'fetch_time', 'Spot Price', 'ATM Strike', 'CE OI', 'PE OI', 'CE LTP', 'PE LTP', 'timestamp']]
            st.dataframe(latest_records, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Detailed data table
            st.subheader("📋 Detailed Data")
            
            # Filter options
            col1, col2 = st.columns(2)
            with col1:
                show_columns = st.multiselect(
                    "Select columns to display:",
                    df.columns.tolist(),
                    default=['Symbol', 'expiry_date', 'fetch_time', 'Spot Price', 'ATM Strike', 'CE OI', 'PE OI', 'CE LTP', 'PE LTP', 'CE IV', 'PE IV', 'timestamp']
                )
            
            with col2:
                search_term = st.text_input("Search in data:", "")
            
            # Filter data
            filtered_df = df[show_columns] if show_columns else df
            
            if search_term:
                # Simple search across all string columns
                mask = pd.DataFrame([filtered_df[col].astype(str).str.contains(search_term, case=False, na=False) 
                                   for col in filtered_df.columns]).any()
                filtered_df = filtered_df[mask]
            
            st.dataframe(filtered_df, use_container_width=True)
            
            # Data summary
            st.subheader("📈 Data Summary")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Records", len(df))
                st.metric("Unique Strikes", df['Strike Price'].nunique())
            
            with col2:
                if 'Spot Price' in df.columns:
                    st.metric("Avg Spot Price", f"{df['Spot Price'].mean():.2f}")
                    st.metric("Min Spot Price", f"{df['Spot Price'].min():.2f}")
            
            with col3:
                if 'Spot Price' in df.columns:
                    st.metric("Max Spot Price", f"{df['Spot Price'].max():.2f}")
                if 'CE OI' in df.columns:
                    st.metric("Max CE OI", f"{df['CE OI'].max():,}")
            
            with col4:
                if 'PE OI' in df.columns:
                    st.metric("Max PE OI", f"{df['PE OI'].max():,}")
                if 'CE IV' in df.columns:
                    st.metric("Max CE IV", f"{df['CE IV'].max():.2f}%")
        
        else:
            st.warning(f"No data available for {selected_symbol} on {selected_date}")
    
    # Auto-refresh indicator
    st.sidebar.markdown("---")
    st.sidebar.markdown("🔄 Auto-refresh every 30 seconds")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        📊 Option Chain Dashboard | Real-time Data Visualization | Built with Streamlit & Plotly
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 
