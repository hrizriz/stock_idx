import streamlit as st
import pandas as pd
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database import execute_query_safe, create_fresh_connection, DB_CONFIG

def show():
    """Debug page"""
    st.markdown("# 🔧 Debug Information")
    
    # Connection info
    st.markdown("## 🔗 Connection Info")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Environment Variables")
        for key, value in DB_CONFIG.items():
            if key == 'password':
                st.text(f"{key}: {'***' if value else 'Not Set'}")
            else:
                st.text(f"{key}: {value}")
    
    with col2:
        st.markdown("### Connection Test")
        if st.button("🧪 Test Fresh Connection"):
            conn = create_fresh_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT version();")
                        version = cursor.fetchone()[0]
                    st.success(f"✅ Connection OK")
                    st.text(f"Version: {version[:50]}...")
                    conn.close()
                except Exception as e:
                    st.error(f"❌ Test failed: {e}")
            else:
                st.error("❌ Cannot create connection")
    
    # Available tables
    st.markdown("## 📋 Available Tables")
    tables_query = """
    SELECT table_schema, table_name, 
           CASE WHEN table_type = 'VIEW' THEN 'View' ELSE 'Table' END as type
    FROM information_schema.tables 
    WHERE table_schema IN ('public', 'public_analytics', 'public_core', 'public_staging')
    ORDER BY table_schema, table_name
    """
    
    if st.button("🔍 Load Tables"):
        tables_df = execute_query_safe(tables_query, "Tables Query")
        if not tables_df.empty:
            st.dataframe(tables_df, use_container_width=True)
        else:
            st.warning("⚠️ No tables found or query failed")
    
    # Sample data
    st.markdown("## 📊 Sample Data")
    if st.button("📋 Load Sample Data"):
        sample_query = "SELECT * FROM daily_stock_summary ORDER BY date DESC LIMIT 5"
        sample_df = execute_query_safe(sample_query, "Sample Data Query")
        
        if not sample_df.empty:
            st.dataframe(sample_df, use_container_width=True)
        else:
            st.warning("⚠️ No sample data available")