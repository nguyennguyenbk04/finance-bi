from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import base64
import json
import shutil
import os
import time

MINIO_ENDPOINT = "http://localhost:9900"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# Initialize Spark
spark = SparkSession.builder \
    .appName("CDC_Stream_Processing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "io.delta:delta-spark_2.13:4.0.0,"
            "org.apache.hadoop:hadoop-aws:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def get_debezium_schema(table_name):
    """Get Debezium-compatible schema for each table"""
    table_schemas = {
        "transactions": """
            transaction_id:bigint,
            trans_date:bigint,
            client_id:bigint,
            card_id:bigint,
            amount:decimal(10,2),
            use_chip:string,
            merchant_id:int,
            mcc:bigint,
            merchant_city:string,
            merchant_state:string,
            zip:string,
            errors:string
        """,
        "mcc_codes": """
            mcc:bigint,
            merchant_type:string
        """,
        "users": """
            client_id:bigint,
            current_age:int,
            retirement_age:int,
            birth_year:int,
            birth_month:int,
            gender:string,
            address:string,
            latitude:decimal(9,6),
            longitude:decimal(9,6),
            per_capita_income:decimal(10,2),
            yearly_income:decimal(10,2),
            total_debt:decimal(10,2),
            credit_score:int,
            num_credit_cards:int
        """,
        "cards": """
            card_id:bigint,
            client_id:bigint,
            card_brand:string,
            card_type:string,
            card_number:string,
            expires:string,
            cvv:string,
            has_chip:string,
            num_cards_issued:int,
            credit_limit:decimal(10,2),
            acct_open_date:string,
            year_pin_last_changed:string,
            card_on_dark_web:string
        """
    }
    return table_schemas.get(table_name, "")

def get_table_columns(table_name):
    """Get column list for each table"""
    columns = {
        "transactions": ["transaction_id", "trans_date", "client_id", "card_id", "amount", 
                        "use_chip", "merchant_id", "mcc", "merchant_city", "merchant_state", "zip", "errors"],
        "mcc_codes": ["mcc", "merchant_type"],
        "users": ["client_id", "current_age", "retirement_age", "birth_year", "birth_month",
                 "gender", "address", "latitude", "longitude", "per_capita_income", 
                 "yearly_income", "total_debt", "credit_score", "num_credit_cards"],
        "cards": ["card_id", "client_id", "card_brand", "card_type", "card_number", "expires",
                 "cvv", "has_chip", "num_cards_issued", "credit_limit", "acct_open_date", 
                 "year_pin_last_changed", "card_on_dark_web"]
    }
    return columns.get(table_name, [])

def get_table_primary_key(table_name):
    """Get primary key for each table"""
    primary_keys = {
        "transactions": "transaction_id",
        "mcc_codes": "mcc", 
        "users": "client_id",
        "cards": "card_id"
    }
    return primary_keys.get(table_name, "id")

def clean_problematic_fields(df, table_name):
    """Clean problematic base64 encoded fields that cause casting errors"""
    if table_name == "users":
        # For users table, handle base64 encoded decimal fields and cast to proper types
        df = df.withColumn("latitude", 
                          when(col("latitude").rlike("^[A-Za-z0-9+/]*={0,2}$"), lit(0.000000).cast("decimal(9,6)"))
                          .otherwise(col("latitude").cast("decimal(9,6)"))) \
               .withColumn("longitude", 
                          when(col("longitude").rlike("^[A-Za-z0-9+/]*={0,2}$"), lit(0.000000).cast("decimal(9,6)"))
                          .otherwise(col("longitude").cast("decimal(9,6)"))) \
               .withColumn("per_capita_income", 
                          when(col("per_capita_income").rlike("^[A-Za-z0-9+/]*={0,2}$"), lit(0.00).cast("decimal(10,2)"))
                          .otherwise(col("per_capita_income").cast("decimal(10,2)"))) \
               .withColumn("yearly_income", 
                          when(col("yearly_income").rlike("^[A-Za-z0-9+/]*={0,2}$"), lit(0.00).cast("decimal(10,2)"))
                          .otherwise(col("yearly_income").cast("decimal(10,2)"))) \
               .withColumn("total_debt", 
                          when(col("total_debt").rlike("^[A-Za-z0-9+/]*={0,2}$"), lit(0.00).cast("decimal(10,2)"))
                          .otherwise(col("total_debt").cast("decimal(10,2)")))
    
    if table_name == "cards":
        # For cards table, handle decimal fields
        df = df.withColumn("credit_limit", 
                          when(col("credit_limit").rlike("^[A-Za-z0-9+/]*={0,2}$"), lit(0.00).cast("decimal(10,2)"))
                          .otherwise(col("credit_limit").cast("decimal(10,2)")))
    
    if table_name == "transactions":
        # For transactions table, handle amount field and convert trans_date from milliseconds to timestamp
        df = df.withColumn("amount", 
                          when(col("amount").rlike("^[A-Za-z0-9+/]*={0,2}$"), lit(0.00).cast("decimal(10,2)"))
                          .otherwise(col("amount").cast("decimal(10,2)"))) \
               .withColumn("trans_date",
                          # Convert Debezium milliseconds timestamp to proper timestamp
                          # Debezium MySQL connector sends DATETIME as milliseconds since epoch
                          when(col("trans_date").isNotNull(), 
                               (col("trans_date") / 1000).cast("timestamp"))
                          .otherwise(lit(None).cast("timestamp")))
    
    return df

def upsert_to_delta(batch_df, batch_id, delta_path, table_name):
    """UPSERT batch DataFrame to Delta table with DELETE support"""
    from delta.tables import DeltaTable
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window
    
    print(f"[{table_name}] Batch {batch_id + 1}: Processing {batch_df.count()} records")
    batch_df.show()
    
    if batch_df.count() == 0:
        return
    
    # Clean problematic fields that might cause casting errors
    batch_df = clean_problematic_fields(batch_df, table_name)
    
    # Get primary key for this table
    primary_key = get_table_primary_key(table_name)
    
    # Deduplicate source data - prioritize DELETE operations over INSERT/UPDATE
    window_spec = Window.partitionBy(primary_key).orderBy(
        # Priority: DELETE (d) operations first, then CREATE/UPDATE (c/u)
        when(col("_operation") == "d", 1).otherwise(2),
        col("_operation")  # Secondary sort for consistency
    )
    
    # Take only the first (highest priority) row for each key
    deduped_batch = batch_df.withColumn("row_num", row_number().over(window_spec)) \
                           .filter(col("row_num") == 1) \
                           .drop("row_num")
    
    print(f"[{table_name}] After deduplication: {deduped_batch.count()} records")
    deduped_batch.show()
    
    # Check if Delta table exists
    try:
        delta_table = DeltaTable.forPath(spark, delta_path)
        print(f"[{table_name}] Batch {batch_id + 1}: Delta table exists, performing MERGE")
    except:
        print(f"[{table_name}] Batch {batch_id + 1}: Creating new Delta table")
        # Filter out deleted records for initial table creation
        initial_data = deduped_batch.filter(col("_deleted") == False).drop("_operation", "_deleted")
        if initial_data.count() > 0:
            initial_data.write.format("delta").save(delta_path)
        return
    
    # Perform UPSERT operation using deduplicated data
    delta_table.alias("target").merge(
        deduped_batch.alias("source"),
        f"target.{primary_key} = source.{primary_key}"
    ).whenMatchedUpdate(
        condition="source._deleted = false",
        set={col: f"source.{col}" for col in deduped_batch.columns if not col.startswith("_")}
    ).whenMatchedDelete(
        condition="source._deleted = true"
    ).whenNotMatchedInsert(
        condition="source._deleted = false",
        values={col: f"source.{col}" for col in deduped_batch.columns if not col.startswith("_")}
    ).execute()

def create_table_stream(table_name):
    """Create streaming query for a specific table"""
    
    print(f"Creating stream for table: {table_name}")
    
    # Read from Kafka topic - CHANGED: Use "latest" instead of "earliest" to process only new records
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", f"finance.finance.{table_name}") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON payload
    table_schema = get_debezium_schema(table_name)
    
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), 
                 f"""
                 schema string,
                 payload struct<
                     before:struct<{table_schema}>,
                     after:struct<{table_schema}>,
                     op:string,
                     ts_ms:bigint,
                     ts_us:bigint,
                     ts_ns:bigint,
                     source:struct<
                         version:string,
                         connector:string,
                         name:string,
                         ts_ms:bigint,
                         snapshot:string,
                         db:string,
                         table:string,
                         server_id:bigint,
                         gtid:string,
                         file:string,
                         pos:bigint,
                         row:int,
                         thread:bigint,
                         query:string
                     >,
                     transaction:string
                 >
                 """
        ).alias("data"),
        col("timestamp")
    )
    
    # Handle INSERTs and UPDATEs (use 'after' data)
    columns = get_table_columns(table_name)
    insert_update_df = parsed_df.filter(
        (col("data.payload.op") == "c") | (col("data.payload.op") == "u")
    ).select(
        # Dynamically select all fields from the 'after' payload
        *[col(f"data.payload.after.{column}") for column in columns],
        col("data.payload.op").alias("_operation"),
        lit(False).alias("_deleted")
    )
    
    # Handle DELETEs (use 'before' data and mark as deleted)
    delete_df = parsed_df.filter(col("data.payload.op") == "d").select(
        # Dynamically select all fields from the 'before' payload
        *[col(f"data.payload.before.{column}") for column in columns],
        col("data.payload.op").alias("_operation"),
        lit(True).alias("_deleted")
    )
    
    # Union INSERT/UPDATE and DELETE operations
    all_operations_df = insert_update_df.unionByName(delete_df)
    
    # Use unique checkpoint path with timestamp to ensure fresh start from latest offset
    checkpoint_path = f"/tmp/checkpoints/{table_name}_cdc_{int(time.time())}"
    
    # Remove old checkpoint if it exists (force fresh start from latest)
    if os.path.exists(checkpoint_path):
        shutil.rmtree(checkpoint_path)
        print(f"[{table_name}] Cleared old checkpoint at {checkpoint_path}")
    
    # Write to Delta Lake using UPSERT pattern
    query = all_operations_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: upsert_to_delta(batch_df, batch_id, f"s3a://rootdb/{table_name}/", table_name)) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    return query

def start_all_table_streams():
    """Start CDC streaming for all tables"""
    
    # Clear all old checkpoints to ensure fresh start from latest offset
    checkpoint_base = "/tmp/checkpoints/"
    if os.path.exists(checkpoint_base):
        print(f"Clearing all old checkpoints from {checkpoint_base}")
        shutil.rmtree(checkpoint_base)
        os.makedirs(checkpoint_base)
        print("Old checkpoints cleared successfully")
    
    # Define all tables to process
    tables = ["transactions", "mcc_codes", "users", "cards"]
    
    queries = []
    
    print("Starting CDC streaming for all tables (processing only NEW records after script start)...")
    
    for table_name in tables:
        try:
            query = create_table_stream(table_name)
            queries.append((table_name, query))
            print(f"Started streaming for {table_name}")
        except Exception as e:
            print(f"Failed to start streaming for {table_name}: {e}")
    
    return queries

if __name__ == "__main__":
    # Start all streams
    active_queries = start_all_table_streams()
    
    print(f"Started {len(active_queries)} streaming queries")
    
    try:
        # Keep all streams running
        print("Streams are running. Press Ctrl+C to stop...")
        for table_name, query in active_queries:
            print(f"- {table_name}: {query.id}")
        
        # Wait for termination (this will run indefinitely until interrupted)
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\nShutting down streams...")
        for table_name, query in active_queries:
            query.stop()
            print(f"✓ Stopped {table_name}")
        print("All streams stopped.")
    except Exception as e:
        print(f"Error in streaming: {e}")
        for table_name, query in active_queries:
            query.stop()
    finally:
        print("CDC streaming session ended.")