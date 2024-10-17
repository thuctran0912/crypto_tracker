import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session
import time
from datetime import datetime, timedelta

st.set_page_config(layout="wide")
session = get_active_session()

def get_crypto_data(crypto):
    try:
        query = f"""
        SELECT TRADE_TIME, AVG_PRICE 
        FROM MSK_STREAMING_DB.MSK_STREAMING_SCHEMA.{crypto}_TRADING_VIEW
        WHERE TRADE_TIME > DATEADD(minutes, -30, CURRENT_TIMESTAMP())
        ORDER BY TRADE_TIME DESC
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching {crypto} data: {str(e)}")
        return pd.DataFrame()

def create_chart(data, title):
    if data.empty:
        return alt.Chart().mark_text().encode(text=alt.value(f"No data available for {title}"))
    
    price_range = data['AVG_PRICE'].max() - data['AVG_PRICE'].min()
    y_min = data['AVG_PRICE'].min() - price_range * 0.1
    y_max = data['AVG_PRICE'].max() + price_range * 0.1

    return alt.Chart(data).mark_line().encode(
        x=alt.X('TRADE_TIME:T', axis=alt.Axis(format='%H:%M:%S', title='Trade Time')),
        y=alt.Y('AVG_PRICE:Q', scale=alt.Scale(domain=[y_min, y_max])),
        tooltip=['TRADE_TIME', 'AVG_PRICE']
    ).properties(title=title).interactive()

st.title("Live Crypto Price Trends")

update_interval = st.sidebar.slider("Update interval (seconds)", 1, 60, 5)
chart_placeholder = st.empty()

while True:
    btc_data = get_crypto_data("BTC")
    eth_data = get_crypto_data("ETH")

    with chart_placeholder.container():
        col1, col2 = st.columns(2)
        with col1:
            st.altair_chart(create_chart(btc_data, "BTC Price Trend"), use_container_width=True)
        with col2:
            st.altair_chart(create_chart(eth_data, "ETH Price Trend"), use_container_width=True)

    time.sleep(update_interval)