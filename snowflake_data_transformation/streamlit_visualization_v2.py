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
        LIMIT 3000
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching {crypto} data: {str(e)}")
        return pd.DataFrame()

def get_current_price(crypto):
    try:
        query = f"""
        SELECT AVG_PRICE, TRADE_TIME
        FROM MSK_STREAMING_DB.MSK_STREAMING_SCHEMA.{crypto}_TRADING_VIEW
        ORDER BY TRADE_TIME DESC
        LIMIT 1
        """
        result = session.sql(query).collect()
        if result:
            return result[0]['AVG_PRICE'], result[0]['TRADE_TIME']
        else:
            return None, None
    except Exception as e:
        st.error(f"Error fetching current {crypto} price: {str(e)}")
        return None, None

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

update_interval = st.sidebar.slider("Chart update interval (seconds)", 1, 60, 5)
chart_placeholder = st.empty()
price_placeholder = st.empty()

last_price_update = datetime.now() - timedelta(minutes=1)  # Initialize to update immediately

while True:
    btc_data = get_crypto_data("BTC")
    eth_data = get_crypto_data("ETH")
    
    # Update current prices every minute
    current_time = datetime.now()
    if current_time - last_price_update >= timedelta(minutes=1):
        btc_price, btc_time = get_current_price("BTC")
        eth_price, eth_time = get_current_price("ETH")
        
        with price_placeholder.container():
            col1, col2, col3 = st.columns(3)
            if btc_price is not None and btc_time is not None:
                col1.metric("Current Bitcoin (BTC) Price", f"${btc_price:.2f}")
            if eth_price is not None and eth_time is not None:
                col2.metric("Current Ethereum (ETH) Price", f"${eth_price:.2f}")
            
            # Display the timestamp of the most recent price data
            if btc_time and eth_time:
                latest_time = max(btc_time, eth_time)
                col3.text(f"Price data as of: {latest_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            elif btc_time:
                col3.text(f"Price data as of: {btc_time.strftime('%Y-%m-%d %H:%M:%S')} UTC ")
            elif eth_time:
                col3.text(f"Price data as of: {eth_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        last_price_update = current_time

    with chart_placeholder.container():
        col1, col2 = st.columns(2)
        with col1:
            st.altair_chart(create_chart(btc_data, "BTC Price Trend"), use_container_width=True)
        with col2:
            st.altair_chart(create_chart(eth_data, "ETH Price Trend"), use_container_width=True)

    time.sleep(update_interval)