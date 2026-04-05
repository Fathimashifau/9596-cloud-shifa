import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv

load_dotenv()

st.title("Real-Time News Dashboard")

refresh_rate = st.sidebar.slider("Refresh every (seconds)", 5, 60, 10)
placeholder = st.empty()

while True:
    with placeholder.container():
        try:
            engine = create_engine(os.environ.get("DATABASE_URL"))
            df = pd.read_sql("SELECT * FROM news_articles ORDER BY fetched_at DESC", engine)
            st.success(f"✅ {len(df)} articles — Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")
            st.dataframe(df)
        except Exception as e:
            st.error(f"❌ Error: {e}")
    time.sleep(refresh_rate)
    st.rerun()
