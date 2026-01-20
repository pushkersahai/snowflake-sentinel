import streamlit as st
import pandas as pd
from datetime import datetime
from sentinel_crew import SnowflakeSentinelCrew
import os
from demo_data import DEMO_RESULTS

# Demo mode configuration
DEMO_MODE = True  # Set to False for live Snowflake/Claude APIs

# Load secrets for Streamlit Cloud deployment
if hasattr(st, 'secrets'):
    try:
        for key in st.secrets.keys():
            os.environ[key] = str(st.secrets[key])
    except:
        pass

st.set_page_config(
    page_title="Snowflake Sentinel",
    page_icon="🎯",
    layout="wide"
)

def init_session_state():
    """Initialize session state variables"""
    if not DEMO_MODE:
        if 'crew' not in st.session_state:
            st.session_state.crew = SnowflakeSentinelCrew()
    if 'results' not in st.session_state:
        st.session_state.results = None
    if 'selected_task' not in st.session_state:
        st.session_state.selected_task = None

def run_detection():
    """Run the detection and investigation workflow"""
    if DEMO_MODE:
        with st.spinner('Loading demo data...'):
            import time
            time.sleep(1)  # Simulate processing time
            st.session_state.results = DEMO_RESULTS
        return DEMO_RESULTS
    else:
        with st.spinner('Detecting failures and running AI analysis...'):
            results = st.session_state.crew.run_investigation()
            st.session_state.results = results
        return results

def extract_fixed_sql(investigation):
    """Extract fixed SQL from investigation results"""
    # First try the parsed fixed_sql field
    if 'fixed_sql' in investigation and investigation['fixed_sql']:
        return investigation['fixed_sql']
    
    # Fallback to parsing diagnosis text
    diagnosis_text = investigation.get('diagnosis', '')
    if not diagnosis_text:
        return None
    
    if "```sql" in diagnosis_text:
        parts = diagnosis_text.split("```sql")
        if len(parts) > 1:
            sql_section = parts[1].split("```")[0]
            return sql_section.strip()
    
    return None

def main():
    init_session_state()
    
    st.title("🎯 Snowflake Sentinel")
    st.markdown("### AI-Powered Task Healing & Cost Optimization")
    
    if DEMO_MODE:
        st.warning("⚠️ **Demo Mode** - Showing cached results from actual analysis runs. No live API calls are made. [View source code on GitHub](https://github.com/pushkersahai/snowflake-sentinel)")
    
    st.markdown("---")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        if st.button("🔄 Scan for Failures", type="primary", use_container_width=True):
            st.session_state.results = None
            st.session_state.selected_task = None
            run_detection()
            st.rerun()
    
    if st.session_state.results is None:
        st.info("Click 'Scan for Failures' to detect and analyze failed Snowflake tasks")
        return
    
    results = st.session_state.results
    
    if len(results) == 0:
        st.success("All tasks are healthy. No failures detected.")
        return
    
    total_savings = sum([r['savings']['annual_cost_saved_usd'] for r in results])
    
    st.markdown("## 💰 Pulse Metric")
    metric_col1, metric_col2, metric_col3 = st.columns(3)
    
    with metric_col1:
        st.metric(
            label="Total Annual Savings",
            value=f"${total_savings:,.2f}",
            delta="From AI-proposed fixes"
        )
    
    with metric_col2:
        st.metric(
            label="Failed Tasks Detected",
            value=len(results),
            delta="Requiring attention"
        )
    
    with metric_col3:
        avg_improvement = sum([r['savings']['estimated_improvement_pct'] for r in results]) / len(results)
        st.metric(
            label="Avg Performance Improvement",
            value=f"{avg_improvement:.0f}%",
            delta="Estimated"
        )
    
    st.markdown("---")
    
    st.markdown("## 📋 Incident Feed")
    
    for idx, result in enumerate(results):
        task_name = result['task_name']
        error_msg = result['error_message']
        savings = result['savings']['annual_cost_saved_usd']
        
        with st.container():
            col1, col2, col3 = st.columns([3, 2, 1])
            
            with col1:
                st.markdown(f"**⚠️ {task_name}**")
                st.caption(f"Error: {error_msg[:100]}...")
            
            with col2:
                st.markdown(f"**Potential Savings:** ${savings:,.2f}/year")
            
            with col3:
                if st.button("🔍 Investigate", key=f"investigate_{idx}"):
                    st.session_state.selected_task = idx
                    st.rerun()
            
            st.markdown("---")
    
    if st.session_state.selected_task is not None:
        selected = results[st.session_state.selected_task]
        
        st.markdown("## 🔍 Forensic Analysis")
        
        st.markdown(f"### Task: `{selected['task_name']}`")
        
        investigation = selected['investigation']
        fixed_sql = extract_fixed_sql(investigation)
        original_sql = investigation['query_text']
        
        st.markdown("#### 🤖 Agent Reasoning Chain")
        
        reasoning_steps = investigation.get('reasoning_steps', {})
        
        if reasoning_steps and any(reasoning_steps.values()):
            # Step 1: Analyze Error
            with st.expander("**STEP 1: Analyze Error**", expanded=True):
                step1 = reasoning_steps.get('step1_analyze_error', 'Not available')
                st.markdown(step1)
            
            # Step 2: Context Check
            with st.expander("**STEP 2: Context Check**"):
                step2 = reasoning_steps.get('step2_context_check', 'Not available')
                st.markdown(step2)
            
            # Step 3: Root Cause
            with st.expander("**STEP 3: Root Cause Identification**", expanded=True):
                step3 = reasoning_steps.get('step3_root_cause', 'Not available')
                st.markdown(step3)
            
            # Step 4: Proposed Fix
            with st.expander("**STEP 4: Propose Fix**", expanded=True):
                step4 = reasoning_steps.get('step4_propose_fix', 'Not available')
                st.markdown(step4)
            
            # Step 5: Validation
            with st.expander("**STEP 5: Validation**"):
                step5 = reasoning_steps.get('step5_validation', 'Not available')
                st.markdown(step5)
        else:
            # Fallback to old format
            diagnosis_text = investigation.get('diagnosis', 'Analysis not available')
            st.info(diagnosis_text)
        
        st.markdown("---")
        
        st.markdown("#### SQL Comparison")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**❌ Broken SQL**")
            st.code(original_sql, language="sql")
        
        with col2:
            st.markdown("**✅ Healed SQL**")
            if fixed_sql:
                st.code(fixed_sql, language="sql")
            else:
                st.warning("No automated fix available. Manual intervention required.")
        
        st.markdown("---")
        
        st.markdown("#### 💰 Financial Impact")
        
        savings_data = selected['savings']
        
        impact_col1, impact_col2, impact_col3, impact_col4 = st.columns(4)
        
        with impact_col1:
            st.metric("Warehouse Size", savings_data['warehouse_size'])
        
        with impact_col2:
            st.metric("Current Runtime", f"{savings_data['avg_execution_time_seconds']:.2f}s")
        
        with impact_col3:
            st.metric("Improvement", f"{savings_data['estimated_improvement_pct']}%")
        
        with impact_col4:
            st.metric("Annual Executions", f"{savings_data['executions_per_year']:,}")
        
        st.markdown("---")
        
        st.markdown("#### 🚀 Deployment")
        
        deploy_col1, deploy_col2 = st.columns([1, 3])
        
        with deploy_col1:
            if fixed_sql and st.button("✅ Approve & Deploy Fix", type="primary", use_container_width=True):
                st.success(f"Fix approved for {selected['task_name']}")
                if DEMO_MODE:
                    st.info("Demo Mode: In production, this would execute the fixed SQL in Snowflake and update the task definition.")
                else:
                    st.info("In production, this would execute the fixed SQL in Snowflake")
                st.balloons()
        
        with deploy_col2:
            if st.button("❌ Reject Fix", use_container_width=True):
                st.warning("Fix rejected. Task will continue to fail until manually resolved.")
        
        st.markdown("---")
        
        if st.button("← Back to Incident Feed"):
            st.session_state.selected_task = None
            st.rerun()

if __name__ == "__main__":
    main()