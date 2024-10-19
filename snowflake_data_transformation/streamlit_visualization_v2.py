import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session
import time
from datetime import datetime, timedelta
from decimal import Decimal

# Configure Streamlit page layout to wide
st.set_page_config(layout="wide")
session = get_active_session()

def get_combined_crypto_data(crypto):
    """
    Fetches historical and latest crypto price data from Snowflake.
    
    Parameters:
    - crypto: The cryptocurrency symbol (e.g., 'BTC', 'ETH').

    Returns:
    - historical_data: DataFrame containing historical price data.
    - latest_price: The latest price of the cryptocurrency.
    - latest_trade_time: The timestamp of the latest trade.
    """
    try:
        query = f"""
        SELECT TRADE_TIME, AVG_PRICE 
        FROM MSK_STREAMING_DB.MSK_STREAMING_SCHEMA.{crypto}_TRADING_VIEW
        WHERE TRADE_TIME >= (CURRENT_TIMESTAMP() - INTERVAL '30 seconds')
        ORDER BY TRADE_TIME ASC
        """
        # Fetch historical data
        historical_data = session.sql(query).to_pandas()

        # Fetch the latest price
        latest_query = f"""
        SELECT AVG_PRICE, TRADE_TIME
        FROM MSK_STREAMING_DB.MSK_STREAMING_SCHEMA.{crypto}_TRADING_VIEW
        ORDER BY TRADE_TIME DESC
        LIMIT 1
        """
        latest_result = session.sql(latest_query).collect()

        if latest_result:
            latest_price = latest_result[0]['AVG_PRICE']
            latest_trade_time = pd.to_datetime(latest_result[0]['TRADE_TIME'])
            return historical_data, latest_price, latest_trade_time
        else:
            return historical_data, None, None
    except Exception as e:
        st.error(f"Error fetching {crypto} data: {str(e)}")
        return pd.DataFrame(), None, None

def create_chart(data, title):
    """
    Creates an Altair line chart for the given data.

    Parameters:
    - data: DataFrame containing price data.
    - title: Title for the chart.

    Returns:
    - An Altair chart object.
    """
    if data.empty:
        return alt.Chart().mark_text().encode(text=alt.value(f"No data available for {title}"))

    price_range = data['AVG_PRICE'].max() - data['AVG_PRICE'].min()
    y_min = data['AVG_PRICE'].min() - price_range * 0.1
    y_max = data['AVG_PRICE'].max() + price_range * 0.1

    # Create the line chart
    return alt.Chart(data).mark_line(color='blue').encode(
        x=alt.X('TRADE_TIME:T', 
                 axis=alt.Axis(format='%H:%M:%S', title='Trade Time', grid=True)),
        y=alt.Y('AVG_PRICE:Q', scale=alt.Scale(domain=[y_min, y_max]), title='Average Price'),
        tooltip=['TRADE_TIME', 'AVG_PRICE']
    ).properties(title=title).interactive()

def get_news():
    """
    Fetches the latest cryptocurrency-related news from Snowflake.

    Returns:
    - DataFrame containing the latest news data.
    """
    try:
        query = """
        SELECT SOURCE, HEADLINE, URL
        FROM BATCH_DB.HISTORY_FINNHUB.FINNHUB_NEWS
        ORDER BY DATETIME DESC
        LIMIT 10
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching news data: {str(e)}")
        return pd.DataFrame()

def get_portfolio():
    """
    Fetches the user's portfolio data from Snowflake.

    Returns:
    - DataFrame containing portfolio details.
    """
    try:
        query = """
        SELECT SYMBOL, PRICE_PER_UNIT_BOUGHT, QUANTITY
        FROM VISUALIZATION_DB.PORTIFOLIO.PORTFOLIO
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching portfolio data: {str(e)}")
        return pd.DataFrame()

def calculate_portfolio_performance(portfolio, current_prices):
    """
    Calculates the performance metrics for the user's portfolio.

    Parameters:
    - portfolio: DataFrame containing portfolio data.
    - current_prices: Dictionary with current prices of assets.

    Returns:
    - Updated portfolio DataFrame and various performance metrics.
    """
    def to_decimal(value):
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value)) if value is not None else Decimal('0')

    # Convert quantities and prices to Decimal for precision
    portfolio['QUANTITY'] = portfolio['QUANTITY'].apply(to_decimal)
    portfolio['PRICE_PER_UNIT_BOUGHT'] = portfolio['PRICE_PER_UNIT_BOUGHT'].apply(to_decimal)
    portfolio['current_price'] = portfolio['SYMBOL'].map(current_prices).apply(to_decimal)

    # Calculate current value, profit/loss, and percent change
    portfolio['current_value'] = portfolio['QUANTITY'] * portfolio['current_price']
    portfolio['bought_value'] = portfolio['QUANTITY'] * portfolio['PRICE_PER_UNIT_BOUGHT']
    portfolio['profit_loss'] = portfolio['current_value'] - portfolio['bought_value']
    portfolio['percent_change'] = ((portfolio['current_price'] - portfolio['PRICE_PER_UNIT_BOUGHT']) / portfolio['PRICE_PER_UNIT_BOUGHT']) * 100

    # Calculate total performance metrics
    total_profit_loss = portfolio['profit_loss'].sum()
    total_current_value = portfolio['current_value'].sum()
    total_bought_value = portfolio['bought_value'].sum()
    overall_percent_change = ((total_current_value - total_bought_value) / total_bought_value) * 100

    return portfolio, float(total_profit_loss), float(total_current_value), float(overall_percent_change)

# Streamlit app layout and logic
st.title("Live Crypto Price Trends, News, and Portfolio Performance")
update_interval = st.sidebar.slider("Chart update interval (seconds)", 1, 60, 5)

# Create placeholders for dynamic content
chart_placeholder = st.empty()
price_placeholder = st.empty()
news_placeholder = st.empty()
portfolio_placeholder = st.empty()

last_price_update = datetime.now() - timedelta(minutes=1)
last_portfolio_update = datetime.now() - timedelta(minutes=15)

current_prices = {}
previous_portfolio_value = None

# Main loop to update the dashboard
while True:
    with st.spinner("Fetching data..."):
        # Fetch historical and latest data for BTC and ETH
        btc_data, btc_price, btc_time = get_combined_crypto_data("BTC")
        eth_data, eth_price, eth_time = get_combined_crypto_data("ETH")
    
        # Update current prices every minute
        current_time = datetime.now()
        if current_time - last_price_update >= timedelta(minutes=1):
            if btc_price is not None:
                current_prices["BTC"] = Decimal(str(btc_price))
            if eth_price is not None:
                current_prices["ETH"] = Decimal(str(eth_price))

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

        # Create and display the charts for BTC and ETH
        with chart_placeholder.container():
            col1, col2 = st.columns(2)
            with col1:
                st.altair_chart(create_chart(btc_data, "BTC Price Trend"), use_container_width=True)
            with col2:
                st.altair_chart(create_chart(eth_data, "ETH Price Trend"), use_container_width=True)

        # Fetch and display news
        news_data = get_news()
        with news_placeholder.container():
            st.subheader("Latest News")
            for _, row in news_data.iterrows():
                st.markdown(f"**{row['SOURCE']}**: [{row['HEADLINE']}]({row['URL']})")

        # Update portfolio performance every 15 minutes
        if current_time - last_portfolio_update >= timedelta(minutes=15):
            portfolio = get_portfolio()
            updated_portfolio, total_profit_loss, total_current_value, overall_percent_change = calculate_portfolio_performance(portfolio, current_prices)

            with portfolio_placeholder.container():
                st.subheader("Portfolio Performance")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Profit/Loss", f"${total_profit_loss:.2f}")
                col2.metric("Total Current Value", f"${total_current_value:.2f}")
                col3.metric("Overall Change", f"{overall_percent_change:.2f}%")

                if previous_portfolio_value is not None:
                    change_15min = total_current_value - previous_portfolio_value
                    change_percentage = (change_15min / previous_portfolio_value) * 100
                    st.metric("Change in Last 15 Minutes", f"${change_15min:.2f}", f"{change_percentage:.2f}%")

                st.markdown("### Portfolio Details")
                for _, row in updated_portfolio.iterrows():
                    st.markdown(f"**{row['SYMBOL']}**:")
                    col1, col2 = st.columns(2)
                    col1.metric("Quantity", f"{float(row['QUANTITY']):.6f}")
                    col2.metric("Price Change", f"{float(row['percent_change']):.2f}%")

                previous_portfolio_value = total_current_value
            
            last_portfolio_update = current_time

    time.sleep(update_interval)
