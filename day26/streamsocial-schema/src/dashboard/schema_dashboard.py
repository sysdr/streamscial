import streamlit as st
import requests
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import time
from datetime import datetime, timedelta

st.set_page_config(
    page_title="StreamSocial Schema Registry",
    page_icon="📋",
    layout="wide"
)

# Configuration
import os
REGISTRY_URL = os.getenv('REGISTRY_URL', 'http://schema-registry:8001')

def get_schema_metrics():
    """Fetch metrics from schema registry"""
    try:
        response = requests.get(f"{REGISTRY_URL}/metrics", timeout=5)
        if response.status_code == 200:
            return response.json()
        return {}
    except:
        return {}

def get_schema_list():
    """Fetch schema list"""
    try:
        response = requests.get(f"{REGISTRY_URL}/schemas", timeout=5)
        if response.status_code == 200:
            return response.json()
        return {}
    except:
        return {}

def validate_sample_event():
    """Validate a sample event"""
    sample_event = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "profile_updated",
        "user_id": "user123",
        "profile_data": {
            "username": "newuser",
            "email": "user@example.com",
            "bio": "Updated bio",
            "privacy_settings": {
                "profile_visibility": "public",
                "allow_messages": True
            }
        },
        "metadata": {
            "source": "web_app",
            "version": "1.0.0"
        }
    }
    
    try:
        response = requests.post(
            f"{REGISTRY_URL}/validate",
            json={"schema_name": "user.profile.v1", "event": sample_event},
            timeout=5
        )
        return response.json()
    except:
        return {"valid": False, "error": "Connection failed"}

# Main dashboard
st.title("🏪 StreamSocial Schema Registry Dashboard")

# Sidebar
st.sidebar.header("Schema Operations")

if st.sidebar.button("Validate Sample Event"):
    result = validate_sample_event()
    if result["valid"]:
        st.sidebar.success("✅ Validation passed!")
    else:
        st.sidebar.error(f"❌ Validation failed: {result.get('error', 'Unknown error')}")

# Auto-refresh
if st.sidebar.checkbox("Auto-refresh (5s)", value=True):
    time.sleep(5)
    st.experimental_rerun()

# Main content
col1, col2 = st.columns(2)

with col1:
    st.subheader("📋 Registered Schemas")
    schemas = get_schema_list()
    
    if schemas:
        schema_df = pd.DataFrame([
            {
                "Schema": name,
                "Version": info["version"],
                "Compatibility": info["compatibility"],
                "Loaded": info["loaded_at"][:19]
            }
            for name, info in schemas.items()
        ])
        st.dataframe(schema_df, use_container_width=True)
    else:
        st.warning("No schemas loaded")

with col2:
    st.subheader("📊 Validation Metrics")
    metrics = get_schema_metrics()
    
    if metrics:
        success_metrics = {k: v for k, v in metrics.items() if "success" in k}
        error_metrics = {k: v for k, v in metrics.items() if "error" in k}
        
        if success_metrics or error_metrics:
            fig = go.Figure()
            
            if success_metrics:
                fig.add_trace(go.Bar(
                    name="Success",
                    x=list(success_metrics.keys()),
                    y=list(success_metrics.values()),
                    marker_color="green"
                ))
            
            if error_metrics:
                fig.add_trace(go.Bar(
                    name="Errors",
                    x=list(error_metrics.keys()),
                    y=list(error_metrics.values()),
                    marker_color="red"
                ))
            
            fig.update_layout(
                title="Validation Results",
                xaxis_title="Schema",
                yaxis_title="Count",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No validation metrics available")
    else:
        st.info("No metrics available")

# Schema health summary
st.subheader("🎯 System Health")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Active Schemas", len(schemas))

with col2:
    total_validations = sum(metrics.values()) if metrics else 0
    st.metric("Total Validations", total_validations)

with col3:
    success_count = sum(v for k, v in metrics.items() if "success" in k) if metrics else 0
    success_rate = (success_count / total_validations * 100) if total_validations > 0 else 0
    st.metric("Success Rate", f"{success_rate:.1f}%")

with col4:
    error_count = sum(v for k, v in metrics.items() if "error" in k) if metrics else 0
    st.metric("Error Count", error_count)

# Real-time validation test
st.subheader("🧪 Schema Validation Test")

test_schema = st.selectbox("Select Schema", list(schemas.keys()) if schemas else [])

if test_schema:
    st.text_area("Test Event JSON", height=200, placeholder="Paste your JSON event here...")
    
    if st.button("Validate Event"):
        st.info("Validation feature available via API endpoint")
