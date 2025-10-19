import streamlit as st
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import plotly.express as px

# Page configuration
st.set_page_config(
    page_title="Dashboard",
    layout="wide"
)

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_ENDPOINT = "http://minio:9000"

# Initialize Spark session (cached for performance)
@st.cache_resource
def get_spark_session():
    return SparkSession.builder \
        .appName("FinanceDashboard") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

spark = get_spark_session()

# Data loading functions
@st.cache_data(ttl=60)
def get_total_users():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/users/")
        return df.count()
    except Exception as e:
        st.error(f"Error loading users: {e}")
        return None

@st.cache_data(ttl=60)
def get_total_transactions():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/transactions/")
        return df.count()
    except Exception as e:
        st.error(f"Error loading transactions: {e}")
        return None

@st.cache_data(ttl=60)
def get_total_cards():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/cards/")
        return df.count()
    except Exception as e:
        st.error(f"Error loading cards: {e}")
        return None

@st.cache_data(ttl=60)
def get_total_mcc_codes():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/mcc_codes/")
        return df.count()
    except Exception as e:
        st.error(f"Error loading MCC codes: {e}")
        return None

@st.cache_data(ttl=60)
def get_total_unique_merchants():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/transactions/")
        return df.select("merchant_id").distinct().count()
    except Exception as e:
        st.error(f"Error loading unique merchants: {e}")
        return None

@st.cache_data(ttl=30)  # Shorter TTL for recent data
def get_top_5_newest_transactions():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/transactions/")
        # Sort by trans_date descending and limit to 5
        recent = df.orderBy(desc("trans_date")).limit(5).select("transaction_id", "trans_date", "client_id", "amount", "merchant_id", "mcc").toPandas()
        return recent
    except Exception as e:
        st.error(f"Error loading recent transactions: {e}")
        return None

@st.cache_data(ttl=300)
def get_transaction_volume_over_time():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/transactions/")
        # Group by every 4 months
        trends = df.withColumn("year", year("trans_date")) \
                   .withColumn("month", month("trans_date")) \
                   .withColumn("period", floor((col("month") - 1) / 4)) \
                   .groupBy("year", "period").count() \
                   .withColumn("period_label", concat_ws("-", col("year"), (col("period") * 4 + 1).cast("string"), lit("to"), (col("period") * 4 + 4).cast("string"))) \
                   .select("period_label", "count") \
                   .orderBy("year", "period").toPandas()
        return trends
    except Exception as e:
        st.error(f"Error loading transaction volume: {e}")
        return None

@st.cache_data(ttl=300)
def get_top_10_customers():
    try:
        df = spark.read.format("delta").load("s3a://rootdb/transactions/")
        # Use client_id as customer identifier
        top = df.groupBy("client_id").count().orderBy(desc("count")).limit(10).toPandas()
        # Ensure sorted by count descending
        top = top.sort_values(by='count', ascending=False)
        # Convert client_id to string to treat as categorical
        top['client_id'] = top['client_id'].astype(str)
        return top
    except Exception as e:
        st.error(f"Error loading top customers: {e}")
        return None

# New function to check if Delta table has been modified
@st.cache_data(ttl=30)  # Cache this check briefly to avoid over-querying
def get_table_last_modified(table_path):
    try:
        # Query Delta metadata for last modified timestamp
        detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
        last_modified = detail_df.select("lastModified").collect()[0][0]
        return last_modified
    except Exception as e:
        st.error(f"Error checking table modification: {e}")
        return None

# Dashboard title
st.title("Live dashboard")

# Initialize session state for tracking last modified times (if not already set)
if "last_modified_transactions" not in st.session_state:
    st.session_state.last_modified_transactions = None
if "last_modified_users" not in st.session_state:
    st.session_state.last_modified_users = None
# Add for other tables as needed (cards, mcc_codes, etc.)

# Get fresh data
total_users = get_total_users()
total_transactions = get_total_transactions()
total_cards = get_total_cards()
total_mcc = get_total_mcc_codes()
total_merchants = get_total_unique_merchants()

# Display metrics in columns
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    if isinstance(total_users, int):
        st.metric("Total Users", f"{total_users:,}")
    else:
        st.metric("Total Users", "Error")

with col2:
    if isinstance(total_transactions, int):
        st.metric("Total Transactions", f"{total_transactions:,}")
    else:
        st.metric("Total Transactions", "Error")

with col3:
    if isinstance(total_cards, int):
        st.metric("Total Cards", f"{total_cards:,}")
    else:
        st.metric("Total Cards", "Error")

with col4:
    if isinstance(total_mcc, int):
        st.metric("Total MCC Codes", f"{total_mcc:,}")
    else:
        st.metric("Total MCC Codes", "Error")

with col5:
    if isinstance(total_merchants, int):
        st.metric("Total Unique Merchants", f"{total_merchants:,}")
    else:
        st.metric("Total Unique Merchants", "Error")

# Recent Transactions section
st.header("Top 5 Newest Transactions")
recent = get_top_5_newest_transactions()
if recent is not None and not recent.empty:
    st.table(recent)
else:
    st.warning("No recent transactions available.")

# Status messages
if isinstance(total_users, int) and total_users > 0:
    st.success("Successfully connected")
elif isinstance(total_users, int) and total_users == 0:
    st.warning("No user data found. Check your Delta Lake table.")
else:
    st.error("Failed to load user data")

st.header("Top 10 Customers by Transaction Count")
customers = get_top_10_customers()
if customers is not None and not customers.empty:
    # Sort by count to ensure proper order from top to bottom
    customers = customers.sort_values(by='count', ascending=True)
    fig = px.bar(customers, x="count", y="client_id", orientation='h', title="Top 10 Customers")
    fig.update_layout(yaxis={'type': 'category'})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No customer data available.") 

st.header("Transaction Volume Over Time")
trends = get_transaction_volume_over_time()
if trends is not None and not trends.empty:
    fig = px.line(trends, x="period_label", y="count", title="Transaction Volume Every 4 Months")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No transaction volume data available.")

# Auto-refresh with change detection
while True:
    # Check for changes in key tables (e.g., transactions and users)
    current_modified_transactions = get_table_last_modified("s3a://rootdb/transactions/")
    current_modified_users = get_table_last_modified("s3a://rootdb/users/")
    
    # If any table has changed, clear caches and rerun
    if (current_modified_transactions != st.session_state.last_modified_transactions or
        current_modified_users != st.session_state.last_modified_users):
        # Update session state
        st.session_state.last_modified_transactions = current_modified_transactions
        st.session_state.last_modified_users = current_modified_users
        # Clear all cached data to force reload
        get_total_users.clear()
        get_total_transactions.clear()
        get_total_cards.clear()
        get_total_mcc_codes.clear()
        get_total_unique_merchants.clear()
        get_top_5_newest_transactions.clear()
        get_transaction_volume_over_time.clear()
        get_top_10_customers.clear()
        get_table_last_modified.clear()  # Clear its own cache too
        st.rerun()  # Only rerun if changes detected
    else:
        time.sleep(5)  # Wait 5 seconds before checking again