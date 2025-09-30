# This demo is not supported in SiS. You must run this streamlit locally.

import json
import re
from typing import Any, Generator, Iterator

import pandas
import pandas as pd
import requests
import snowflake.connector
import sseclient
import streamlit as st
from dotenv import load_dotenv
load_dotenv()
import os
import base64
import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# Load environment variables
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FORECAST_WH")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FORECAST_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA")
PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
PRIVATE_KEY_B64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")
SNOWFLAKE_FILE = "semantics.yaml"
SNOWFLAKE_STAGE = "SEMANTICS"

# Page configuration
st.set_page_config(
    page_title="Upside Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    /* Core Colors */
    :root {
        --primary-color: #FFFFFF;
        --secondary-color: #E5E7EB;
        --accent-color: #3B82F6;
        --text-color: #FFFFFF;
        --background-color: #000000;
        --card-bg: #1A1A1A;
        --border-color: #333333;
    }

    /* Main app background */
    .stApp {
        background-color: var(--background-color);
        color: var(--text-color);
        font-family: "Inter", "Segoe UI", Arial, sans-serif;
    }

    /* Header */
    .main-header {
        background: var(--card-bg);
        padding: 2rem;
        border-radius: 8px;
        margin-bottom: 2rem;
        border: 1px solid var(--border-color);
    }

    .main-title {
        color: var(--primary-color);
        font-size: 2rem;
        font-weight: 600;
        text-align: center;
        margin: 0;
        letter-spacing: -0.5px;
    }

    .main-subtitle {
        color: var(--secondary-color);
        font-size: 1rem;
        text-align: center;
        margin-top: 0.5rem;
        font-weight: 400;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: var(--card-bg);
        color: var(--text-color);
        border-right: 1px solid var(--border-color);
    }

    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2, 
    [data-testid="stSidebar"] h3 {
        color: var(--primary-color);
    }

    [data-testid="stSidebar"] p, 
    [data-testid="stSidebar"] li {
        color: var(--secondary-color);
    }

    /* Cards */
    .quick-questions {
        background: var(--card-bg);
        padding: 1.5rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
        border: 1px solid var(--border-color);
    }

    .quick-questions h3 {
        color: var(--primary-color);
        font-size: 1.2rem;
        margin-bottom: 0.5rem;
        font-weight: 600;
    }

    .quick-questions p {
        color: var(--secondary-color);
    }

    /* Chat Messages */
    .stChatMessage {
        background: var(--card-bg);
        border: 1px solid var(--border-color);
        color: var(--text-color);
    }

    /* Buttons */
    .stButton > button {
        background-color: var(--card-bg);
        color: var(--primary-color);
        border: 1px solid var(--border-color);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        font-weight: 500;
        transition: all 0.2s ease;
        width: 100%;
        text-align: left;
    }

    .stButton > button:hover {
        background-color: var(--accent-color);
        color: var(--primary-color);
        border-color: var(--accent-color);
        transform: translateY(-1px);
    }

    /* Input */
    .stChatInputContainer {
        border-top: 1px solid var(--border-color);
    }

    /* Tables / Dataframes */
    .dataframe {
        background-color: var(--card-bg);
        color: var(--text-color);
        border: 1px solid var(--border-color);
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: var(--background-color);
    }

    .stTabs [data-baseweb="tab"] {
        background-color: var(--card-bg);
        color: var(--secondary-color);
        border: 1px solid var(--border-color);
        border-radius: 4px;
    }

    .stTabs [aria-selected="true"] {
        background-color: var(--accent-color);
        color: var(--primary-color);
    }

    /* Metric styling */
    [data-testid="stMetricValue"] {
        color: var(--primary-color);
        font-size: 1.6rem;
        font-weight: 600;
    }

    /* Info box */
    .stAlert {
        background-color: var(--card-bg);
        color: var(--text-color);
        border: 1px solid var(--border-color);
    }

    /* Spinner */
    .stSpinner > div {
        border-top-color: var(--accent-color);
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

def _build_conn_params():
    """Build Snowflake connection parameters"""
    missing = []
    if not SNOWFLAKE_ACCOUNT:
        missing.append("SNOWFLAKE_ACCOUNT")
    if not SNOWFLAKE_USER:
        missing.append("SNOWFLAKE_USER")
    if missing:
        raise RuntimeError(
            f"Missing required Snowflake environment variables: {', '.join(missing)}"
        )

    params = {
        "user": SNOWFLAKE_USER,
        "account": SNOWFLAKE_ACCOUNT,
        "warehouse": SNOWFLAKE_WAREHOUSE,
    }

    if SNOWFLAKE_ROLE:
        params["role"] = SNOWFLAKE_ROLE

    # Prefer private key auth if provided
    if PRIVATE_KEY_B64:
        try:
            key_bytes = base64.b64decode(PRIVATE_KEY_B64)
            passphrase = (
                PRIVATE_KEY_PASSPHRASE.encode()
                if PRIVATE_KEY_PASSPHRASE else None
            )
            pkey = serialization.load_pem_private_key(key_bytes, password=passphrase)
            params["private_key"] = pkey.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        except Exception as e:
            raise RuntimeError(f"Failed to load private key from base64 env: {e}")
    elif PRIVATE_KEY_PATH:
        try:
            with open(PRIVATE_KEY_PATH, "rb") as key_file:
                passphrase = (
                    PRIVATE_KEY_PASSPHRASE.encode()
                    if PRIVATE_KEY_PASSPHRASE else None
                )
                pkey = serialization.load_pem_private_key(
                    key_file.read(), password=passphrase
                )
                params["private_key"] = pkey.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
        except Exception as e:
            raise RuntimeError(f"Failed to load private key from file: {e}")
    else:
        if not SNOWFLAKE_PASSWORD:
            raise RuntimeError(
                "SNOWFLAKE_PASSWORD is required if not using private key auth."
            )
        params["password"] = SNOWFLAKE_PASSWORD

    return params


# Create connection only once and keep it in session_state
if "CONN" not in st.session_state or st.session_state.CONN is None:
    st.session_state.CONN = snowflake.connector.connect(**_build_conn_params())



def get_conversation_history() -> list[dict[str, Any]]:
    messages = []
    for msg in st.session_state.messages:
        m: dict[str, Any] = {}
        # Always use 'analyst' role for consistency
        if msg["role"] == "user":
            m["role"] = "user"
        else:
            m["role"] = "analyst"
        text_content = "\n".join([c for c in msg["content"] if isinstance(c, str)])
        m["content"] = [{"type": "text", "text": text_content}]
        messages.append(m)
    return messages


def send_message() -> requests.Response:
    """Calls the REST API and returns a streaming client."""
    request_body = {
        "messages": get_conversation_history(),
        "semantic_model_file": f"@{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}/{SNOWFLAKE_FILE}",
        "stream": True,
    }
    resp = requests.post(
        url=f"https://{st.session_state.CONN.host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True,
    )
    if resp.status_code < 400:
        return resp  # type: ignore
    else:
        raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")


def stream(events: Iterator[sseclient.Event]) -> Generator[Any, Any, Any]:
    prev_index = -1
    prev_type = ""
    prev_suggestion_index = -1
    while True:
        event = next(events, None)
        if not event:
            return
        data = json.loads(event.data)
        new_content_block = event.event != "message.content.delta" or data["index"] != prev_index

        match event.event:
            case "message.content.delta":
                match data["type"]:
                    case "sql":
                        # Skip SQL output - don't yield anything for SQL blocks
                        # Store the SQL query in session state for execution but don't display it
                        if "current_sql" not in st.session_state:
                            st.session_state.current_sql = ""
                        st.session_state.current_sql += data["statement_delta"]
                    case "text":
                        yield data["text_delta"]
                    case "suggestions":
                        if new_content_block:
                            # Add a suggestions header when we enter a new suggestions block.
                            yield "\n\n💡 **Here are some example questions you could ask:**\n\n"
                            yield "\n- "
                        elif (
                            prev_suggestion_index != data["suggestions_delta"]["index"]
                        ):
                            yield "\n- "
                        yield data["suggestions_delta"]["suggestion_delta"]
                        prev_suggestion_index = data["suggestions_delta"]["index"]
                prev_index = data["index"]
                prev_type = data["type"]
            case "status":
                st.session_state.status = data["status_message"]
                # We return here to allow the spinner to update with the latest status, but this method will be
                #  called again for the next iteration
                return
            case "error":
                st.session_state.error = data
                return


def display_df(df: pandas.DataFrame) -> None:
    st.dataframe(df, use_container_width=True)



def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append({"role": "user", "content": [prompt]})
    with st.chat_message("user"):
        st.markdown(prompt)

    accumulated_content = []
    with st.chat_message("assistant"):
        with st.spinner("🔍 Analyzing your question..."):
            response = send_message()
        events = sseclient.SSEClient(response).events()  # type: ignore
        # Initialize current_sql for this request
        st.session_state.current_sql = ""
        
        while st.session_state.status.lower() != "done":
            with st.spinner(f"⚡ {st.session_state.status}"):
                written_content = st.write_stream(stream(events))
                accumulated_content.append(written_content)
            if st.session_state.error:
                st.error(
                    f"❌ Error while processing request:\n {st.session_state.error}",
                    icon="🚨",
                )
                accumulated_content.append(Exception(st.session_state.error))
                st.session_state.error = None
                st.session_state.status = "Analyzing your question"
                st.session_state.messages.pop()
                return
            
            # Execute SQL if we have accumulated any SQL query
            if st.session_state.current_sql.strip():
                with st.spinner("📊 Retrieving data..."):
                    try:
                        df = pd.read_sql(st.session_state.current_sql, st.session_state.CONN)
                        accumulated_content.append(df)
                        display_df(df)
                        # Clear the SQL after execution
                        st.session_state.current_sql = ""
                    except Exception as e:
                        st.error(f"❌ Error executing query: {e}", icon="🚨")
                        
    st.session_state.status = "Analyzing your question"
    st.session_state.messages.append(
        {"role": "analyst", "content": accumulated_content}
    )


def show_conversation_history() -> None:
    for message in st.session_state.messages:
        chat_role = "assistant" if message["role"] == "analyst" else "user"
        with st.chat_message(chat_role):
            for content in message["content"]:
                if isinstance(content, pd.DataFrame):
                    display_df(content)
                elif isinstance(content, Exception):
                    st.error(f"❌ Error while processing request:\n {content}", icon="🚨")
                else:
                    st.write(content)


# Header
st.markdown("""
    <div class="main-header">
        <h1 class="main-title">🛒 FMCG Analytics Intelligence</h1>
        <p class="main-subtitle">Powered by AI-driven insights for smarter business decisions</p>
    </div>
""", unsafe_allow_html=True)

# Sidebar with information
with st.sidebar:
    st.markdown("### 📌 About")
    st.markdown("""
    This intelligent analytics dashboard helps you understand your FMCG business better through:
    
    - 📊 **Sales Performance Analysis**
    - 🏪 **Store & Location Insights**
    - 📈 **Trend Analysis**
    - 💰 **Pricing & Discount Impact**
    - 🚚 **Order & Fulfillment Metrics**
    """)
    
    st.markdown("---")
    st.markdown(f"**Semantic Model:** `{SNOWFLAKE_FILE}`")
    st.markdown("---")
    
    st.markdown("### 💡 Tips")
    st.info("""
    - Ask questions in natural language
    - Be specific about time periods
    - Request comparisons for deeper insights
    - Ask for visualizations when needed
    """)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.status = "Analyzing your question"
    st.session_state.error = None
    st.session_state.current_sql = ""

# Quick Questions Section (only show if no conversation history)
if len(st.session_state.messages) == 0:
    st.markdown("""
        <div class="quick-questions">
            <h3>🚀 Quick Start Questions</h3>
            <p style="color: #666; margin-bottom: 1rem;">Click any question below to get instant insights:</p>
        </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 What are the top 10 best-selling products by quantity?", key="q1"):
            process_message("What are the top 10 best-selling products by quantity?")
            st.rerun()
        
        if st.button("🏪 Which products perform best in Tier 1 cities vs Tier 2 cities?", key="q2"):
            process_message("Which products perform best in Tier 1 cities vs Tier 2 cities?")
            st.rerun()
    
    with col2:
        if st.button("💰 Which products generated the highest revenue last month?", key="q3"):
            process_message("Which products generated the highest revenue last month?")
            st.rerun()
        
        if st.button("📈 Which products had the highest positive month-over-month growth?", key="q4"):
            process_message("Which products had the highest positive month-over-month growth?")
            st.rerun()
    
    st.markdown("<br>", unsafe_allow_html=True)

# Show conversation history
show_conversation_history()

# Chat input
if user_input := st.chat_input("💬 Ask me anything about your FMCG business..."):
    process_message(prompt=user_input)