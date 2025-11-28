"""
Streamlit dashboard for Workforce Shift & Exception Analytics.

Features:
- Hourly workforce heatmap (per facility)
- Exception timeline
- Employee drill-down
- Event timeline replay
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Workforce Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Dynamic Workforce Shift & Exception Analytics")
st.markdown("---")


@st.cache_data
def load_work_sessions(file_path: str = "data/processed/work_sessions.csv"):
    """Load work sessions data."""
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            # Convert timestamp columns
            for col in ['shift_start', 'shift_end', 'actual_start', 'actual_end', 'session_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            return df
        except Exception as e:
            st.error(f"Error loading work sessions: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


@st.cache_data
def load_attendance_events(file_path: str = "data/raw/attendance.csv"):
    """Load attendance events data."""
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            if 'event_timestamp' in df.columns:
                df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
            return df
        except Exception as e:
            st.error(f"Error loading attendance events: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


def create_workforce_heatmap(df: pd.DataFrame):
    """Create hourly workforce heatmap per facility."""
    if df.empty or 'actual_start' not in df.columns:
        return None
    
    # Extract hour from actual_start
    df['hour'] = df['actual_start'].dt.hour
    df['date'] = df['actual_start'].dt.date
    
    # Count employees per hour and facility
    if 'facility' in df.columns:
        heatmap_data = df.groupby(['facility', 'hour', 'date']).size().reset_index(name='count')
        
        # Pivot for heatmap
        pivot_data = heatmap_data.pivot_table(
            index='hour',
            columns='facility',
            values='count',
            aggfunc='mean'
        ).fillna(0)
        
        fig = px.imshow(
            pivot_data.T,
            labels=dict(x="Hour of Day", y="Facility", color="Employee Count"),
            x=[f"{h:02d}:00" for h in range(24)],
            y=pivot_data.columns,
            color_continuous_scale="YlOrRd",
            title="Average Workforce by Hour and Facility"
        )
        fig.update_layout(height=400)
        return fig
    else:
        # Simple hourly heatmap
        hourly_counts = df.groupby('hour').size()
        
        fig = go.Figure(data=go.Bar(
            x=[f"{h:02d}:00" for h in hourly_counts.index],
            y=hourly_counts.values,
            marker_color='steelblue'
        ))
        fig.update_layout(
            title="Average Workforce by Hour",
            xaxis_title="Hour of Day",
            yaxis_title="Employee Count",
            height=400
        )
        return fig


def create_exception_timeline(df: pd.DataFrame):
    """Create exception timeline visualization."""
    if df.empty or 'exception_codes' not in df.columns:
        return None
    
    # Filter sessions with exceptions
    exceptions_df = df[df['exception_codes'].notna() & (df['exception_codes'] != '')].copy()
    
    if exceptions_df.empty:
        return None
    
    # Parse exception codes
    exceptions_df['exception_list'] = exceptions_df['exception_codes'].str.split(',')
    exceptions_df = exceptions_df.explode('exception_list')
    exceptions_df['exception_list'] = exceptions_df['exception_list'].str.strip()
    
    # Count exceptions by date
    if 'session_date' in exceptions_df.columns:
        exception_counts = exceptions_df.groupby(['session_date', 'exception_list']).size().reset_index(name='count')
        
        fig = px.scatter(
            exception_counts,
            x='session_date',
            y='exception_list',
            size='count',
            color='count',
            labels={'session_date': 'Date', 'exception_list': 'Exception Type', 'count': 'Count'},
            title="Exception Timeline",
            color_continuous_scale="Reds"
        )
        fig.update_layout(height=500)
        return fig
    
    return None


def main():
    """Main dashboard function."""
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Load data
    sessions_df = load_work_sessions()
    events_df = load_attendance_events()
    
    if sessions_df.empty:
        st.warning("No work sessions data available. Please run ETL pipeline first.")
        st.info("Run: `python src/etl/etl_spark.py --input data/raw/attendance.csv --output data/processed`")
        return
    
    # Facility filter
    if 'facility' in sessions_df.columns:
        facilities = ['All'] + sorted(sessions_df['facility'].dropna().unique().tolist())
        selected_facility = st.sidebar.selectbox("Facility", facilities)
        
        if selected_facility != 'All':
            sessions_df = sessions_df[sessions_df['facility'] == selected_facility]
            events_df = events_df[events_df['facility'] == selected_facility] if 'facility' in events_df.columns else events_df
    
    # Date range filter
    if 'session_date' in sessions_df.columns:
        min_date = sessions_df['session_date'].min()
        max_date = sessions_df['session_date'].max()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            sessions_df = sessions_df[
                (sessions_df['session_date'] >= pd.Timestamp(date_range[0])) &
                (sessions_df['session_date'] <= pd.Timestamp(date_range[1]))
            ]
    
    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Overview",
        "⚠️ Exceptions",
        "👤 Employee Details",
        "⏱️ Event Timeline"
    ])
    
    with tab1:
        st.header("Workforce Overview")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Sessions", len(sessions_df))
        
        with col2:
            total_hours = sessions_df['worked_hours'].sum() if 'worked_hours' in sessions_df.columns else 0
            st.metric("Total Hours", f"{total_hours:.1f}")
        
        with col3:
            exceptions_count = len(sessions_df[sessions_df['exception_codes'].notna() & (sessions_df['exception_codes'] != '')])
            st.metric("Sessions with Exceptions", exceptions_count)
        
        with col4:
            avg_hours = sessions_df['worked_hours'].mean() if 'worked_hours' in sessions_df.columns else 0
            st.metric("Avg Hours per Session", f"{avg_hours:.1f}")
        
        st.markdown("---")
        
        # Workforce heatmap
        st.subheader("Workforce Heatmap")
        heatmap_fig = create_workforce_heatmap(sessions_df)
        if heatmap_fig:
            st.plotly_chart(heatmap_fig, use_container_width=True)
        else:
            st.info("No data available for heatmap")
        
        # Work hours distribution
        if 'worked_hours' in sessions_df.columns:
            st.subheader("Work Hours Distribution")
            fig = px.histogram(
                sessions_df,
                x='worked_hours',
                nbins=30,
                labels={'worked_hours': 'Worked Hours', 'count': 'Frequency'},
                title="Distribution of Worked Hours"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("Exception Analysis")
        
        # Exception timeline
        exception_timeline = create_exception_timeline(sessions_df)
        if exception_timeline:
            st.plotly_chart(exception_timeline, use_container_width=True)
        else:
            st.info("No exceptions found")
        
        # Exception summary table
        if 'exception_codes' in sessions_df.columns:
            exceptions_df = sessions_df[sessions_df['exception_codes'].notna() & (sessions_df['exception_codes'] != '')].copy()
            
            if not exceptions_df.empty:
                exceptions_df['exception_list'] = exceptions_df['exception_codes'].str.split(',')
                exceptions_df = exceptions_df.explode('exception_list')
                exceptions_df['exception_list'] = exceptions_df['exception_list'].str.strip()
                
                exception_summary = exceptions_df['exception_list'].value_counts().reset_index()
                exception_summary.columns = ['Exception Type', 'Count']
                
                st.subheader("Exception Summary")
                st.dataframe(exception_summary, use_container_width=True)
                
                # Detailed exception table
                st.subheader("Exception Details")
                detailed_df = exceptions_df[['employee_id', 'session_date', 'exception_list', 'exception_explanations']].copy()
                st.dataframe(detailed_df, use_container_width=True)
    
    with tab3:
        st.header("Employee Drill-Down")
        
        # Employee selector
        if 'employee_id' in sessions_df.columns:
            employee_ids = sorted(sessions_df['employee_id'].unique())
            selected_employee = st.selectbox("Select Employee", employee_ids)
            
            emp_sessions = sessions_df[sessions_df['employee_id'] == selected_employee].copy()
            
            if not emp_sessions.empty:
                # Employee summary
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Sessions", len(emp_sessions))
                
                with col2:
                    total_hours = emp_sessions['worked_hours'].sum() if 'worked_hours' in emp_sessions.columns else 0
                    st.metric("Total Hours", f"{total_hours:.1f}")
                
                with col3:
                    exceptions = len(emp_sessions[emp_sessions['exception_codes'].notna() & (emp_sessions['exception_codes'] != '')])
                    st.metric("Exceptions", exceptions)
                
                # Employee sessions table
                st.subheader("Work Sessions")
                display_cols = ['session_date', 'actual_start', 'actual_end', 'worked_hours', 
                              'overtime_hours', 'exception_codes']
                display_cols = [c for c in display_cols if c in emp_sessions.columns]
                st.dataframe(emp_sessions[display_cols], use_container_width=True)
                
                # Exception explanations
                if 'exception_explanations' in emp_sessions.columns:
                    st.subheader("Exception Explanations")
                    for idx, row in emp_sessions.iterrows():
                        if pd.notna(row.get('exception_explanations')):
                            try:
                                expl = json.loads(row['exception_explanations'])
                                for code, msg in expl.items():
                                    st.write(f"**{code}**: {msg}")
                            except:
                                st.write(row['exception_explanations'])
                
                # Employee events (if available)
                if not events_df.empty and 'employee_id' in events_df.columns:
                    emp_events = events_df[events_df['employee_id'] == selected_employee].copy()
                    if not emp_events.empty:
                        st.subheader("Raw Events")
                        st.dataframe(emp_events, use_container_width=True)
    
    with tab4:
        st.header("Event Timeline Replay")
        
        if events_df.empty:
            st.info("No attendance events data available")
        else:
            # Date selector
            if 'event_timestamp' in events_df.columns:
                events_df['date'] = events_df['event_timestamp'].dt.date
                dates = sorted(events_df['date'].unique())
                
                selected_date = st.selectbox("Select Date", dates)
                
                day_events = events_df[events_df['date'] == selected_date].copy()
                day_events = day_events.sort_values('event_timestamp')
                
                if not day_events.empty:
                    st.subheader(f"Events on {selected_date}")
                    
                    # Timeline visualization
                    fig = go.Figure()
                    
                    check_ins = day_events[day_events['event_type'] == 'CHECK_IN']
                    check_outs = day_events[day_events['event_type'] == 'CHECK_OUT']
                    
                    if not check_ins.empty:
                        fig.add_trace(go.Scatter(
                            x=check_ins['event_timestamp'],
                            y=check_ins['employee_id'],
                            mode='markers',
                            name='Check-In',
                            marker=dict(symbol='triangle-up', size=10, color='green')
                        ))
                    
                    if not check_outs.empty:
                        fig.add_trace(go.Scatter(
                            x=check_outs['event_timestamp'],
                            y=check_outs['employee_id'],
                            mode='markers',
                            name='Check-Out',
                            marker=dict(symbol='triangle-down', size=10, color='red')
                        ))
                    
                    fig.update_layout(
                        title="Event Timeline",
                        xaxis_title="Time",
                        yaxis_title="Employee ID",
                        height=600
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Events table
                    st.dataframe(day_events, use_container_width=True)
                else:
                    st.info(f"No events found for {selected_date}")


if __name__ == "__main__":
    main()

