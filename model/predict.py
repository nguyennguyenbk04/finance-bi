from pyspark.sql.functions import col
from pyspark.ml.pipeline import PipelineModel
import sys
from pyspark.sql import SparkSession

APP_NAME = "FraudPrediction"
DRIVER_MEMORY = "8g"
EXECUTOR_MEMORY = "4g"

MODEL_PATH = "sparkFraudRfModel"
OUTPUT_PATH = "prediction_output"

REQUIRED_COLUMNS = [
    "step", "type", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"
]

OUTPUT_COLUMNS = REQUIRED_COLUMNS + ["prediction"]

def initialize_spark():
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.executor.memory", EXECUTOR_MEMORY)
        .getOrCreate()
    )


def load_model():
    return PipelineModel.load(MODEL_PATH)

def engineer_features(data):
    enriched = data.withColumn(
        "origBalanceChange", 
        col("newbalanceOrig") - col("oldbalanceOrg")
    ).withColumn(
        "destBalanceChange", 
        col("newbalanceDest") - col("oldbalanceDest")
    )
    
    numeric_columns = [
        column_name 
        for column_name, data_type in enriched.dtypes 
        if data_type in ("double", "int", "bigint")
    ]
    
    fill_mapping = {column_name: 0 for column_name in numeric_columns}
    return enriched.na.fill(fill_mapping)


def predict(session, trained_model, input_path):
    raw_data = session.read.csv(input_path, header = True, inferSchema = True)
    processed_data = engineer_features(raw_data)
    predictions = trained_model.transform(processed_data)
    
    output_data = predictions.select(*OUTPUT_COLUMNS)
    output_data.write.csv(OUTPUT_PATH, header = True, mode = "overwrite")

def main():
    if len(sys.argv) < 2:
        sys.exit(1)
    
    input_path = sys.argv[1]
    
    session = initialize_spark()
    trained_model = load_model()
    predict(session, trained_model, input_path)
    session.stop()

if __name__ == "__main__":
    main()