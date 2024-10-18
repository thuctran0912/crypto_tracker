# Import libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import altair as alt
import streamlit as st
import pandas as pd
import time  # To introduce delay for data fetching

# Set page config
st.set_page_config(layout="wide")

# Get current session
session = get_active_session()

# Function to fetch BTC price data from Snowflake
def get_btc_price_data():
    query = "SELECT * FROM BTC_PRICE ORDER BY TRADE_TIME DESC"
    df = session.sql(query).to_pandas()
    return df

# Function to fetch ETH price data from Snowflake
def get_eth_price_data():
    query = "SELECT * FROM ETH_PRICE ORDER BY TRADE_TIME DESC"
    df = session.sql(query).to_pandas()
    return df

# Streamlit visualization
st.title("Live Crypto Price Trends")

# Placeholder for the charts
chart_placeholder = st.empty()

while True:
    # Fetch and prepare BTC data
    btc_data = get_btc_price_data()
    btc_price_range = btc_data['AVG_PRICE'].max() - btc_data['AVG_PRICE'].min()
    btc_y_min = btc_data['AVG_PRICE'].min() - btc_price_range * 0.1
    btc_y_max = btc_data['AVG_PRICE'].max() + btc_price_range * 0.1

    # Create BTC chart
    btc_chart = alt.Chart(btc_data).mark_line().encode(
        x=alt.X('TRADE_TIME:T', axis=alt.Axis(format='%H:%M:%S', title='Trade Time')),
        y=alt.Y('AVG_PRICE:Q', scale=alt.Scale(domain=[btc_y_min, btc_y_max])),
        tooltip=['TRADE_TIME', 'AVG_PRICE']
    ).properties(title="BTC Price Trend").interactive()

    # Fetch and prepare ETH data
    eth_data = get_eth_price_data()
    eth_price_range = eth_data['AVG_PRICE'].max() - eth_data['AVG_PRICE'].min()
    eth_y_min = eth_data['AVG_PRICE'].min() - eth_price_range * 0.1
    eth_y_max = eth_data['AVG_PRICE'].max() + eth_price_range * 0.1

    # Create ETH chart
    eth_chart = alt.Chart(eth_data).mark_line().encode(
        x=alt.X('TRADE_TIME:T', axis=alt.Axis(format='%H:%M:%S', title='Trade Time')),
        y=alt.Y('AVG_PRICE:Q', scale=alt.Scale(domain=[eth_y_min, eth_y_max])),
        tooltip=['TRADE_TIME', 'AVG_PRICE']
    ).properties(title="ETH Price Trend").interactive()

    # Clear the placeholder and display both charts vertically
    with chart_placeholder.container():
        st.altair_chart(alt.vconcat(btc_chart, eth_chart), use_container_width=True)

    # Sleep for a few seconds before fetching new data
    time.sleep(5)  # Adjust the sleep duration as needed
