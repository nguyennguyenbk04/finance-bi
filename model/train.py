from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

APP_NAME = "FraudDetectionModel"
DATA_PATH = "PS_20174392719_1491204439457_log.csv"
MODEL_PATH = "sparkFraudRfModel"

REQUIRED_COLUMNS = [
    "step", "type", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "isFraud"
]

NUMERIC_FEATURES = [
    "step", "amount","oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "origBalanceChange", "destBalanceChange"
]

CATEGORICAL_INPUT = "type"
CATEGORICAL_INDEXED = "typeIdx"
CATEGORICAL_ENCODED = "typeVec"
NUMERIC_VECTOR = "numericVec"
RAW_FEATURES = "rawFeatures"
SCALED_FEATURES = "features"
LABEL_COLUMN = "isFraud"
WEIGHT_COLUMN = "classWeightCol"

TRAIN_RATIO = 0.6
TEST_RATIO = 0.4
RANDOM_SEED = 42

NUM_TREES = 20
MAX_DEPTH = 8
MAX_BINS = 32

def load_data(session):
    data = session.read.csv(DATA_PATH, header=True, inferSchema=True)
    return data.select(*REQUIRED_COLUMNS)

def engineer_features(data):
    enriched = data.withColumn(
        "origBalanceChange", 
        col("newbalanceOrig") - col("oldbalanceOrg")
    ).withColumn(
        "destBalanceChange", 
        col("newbalanceDest") - col("oldbalanceDest")
    )
    
    for column_name, data_type in enriched.dtypes:
        if data_type in ("double", "int", "bigint"):
            enriched = enriched.na.fill({column_name: 0})
    
    return enriched


def build_preprocessing_stages():
    categorical_indexer = StringIndexer(
        inputCol = CATEGORICAL_INPUT,
        outputCol = CATEGORICAL_INDEXED,
        handleInvalid = "keep"
    )
    
    categorical_encoder = OneHotEncoder(
        inputCols = [CATEGORICAL_INDEXED],
        outputCols = [CATEGORICAL_ENCODED]
    )
    
    numeric_assembler = VectorAssembler(
        inputCols = NUMERIC_FEATURES,
        outputCol = NUMERIC_VECTOR,
        handleInvalid="keep"
    )
    
    feature_combiner = VectorAssembler(
        inputCols = [NUMERIC_VECTOR, CATEGORICAL_ENCODED],
        outputCol = RAW_FEATURES
    )
    
    feature_scaler = StandardScaler(
        inputCol = RAW_FEATURES,
        outputCol = SCALED_FEATURES,
        withStd = True,
        withMean = False
    )
    
    return [categorical_indexer, categorical_encoder, numeric_assembler, feature_combiner, feature_scaler]

def apply_weights(data):
    label_counts = data.groupBy(LABEL_COLUMN).count().collect()
    count_mapping = {row[LABEL_COLUMN]: row["count"] for row in label_counts}
    
    negative_count = count_mapping.get(0, 1)
    positive_count = count_mapping.get(1, 1)
    total_count = negative_count + positive_count
    
    weight_expression = when(
        col(LABEL_COLUMN) == 1, 
        total_count / (2.0 * positive_count)
    ).otherwise(
        total_count / (2.0 * negative_count)
    )
    
    return data.withColumn(WEIGHT_COLUMN, weight_expression)

def train_model(session):
    dataset = load_data(session)
    dataset = engineer_features(dataset)
    
    training_set, test_set = dataset.randomSplit(
        [TRAIN_RATIO, TEST_RATIO], 
        seed = RANDOM_SEED
    )
    
    training_set = apply_weights(training_set)
    
    preprocessing_stages = build_preprocessing_stages()
    classifier = RandomForestClassifier(
        labelCol = LABEL_COLUMN,
        featuresCol = SCALED_FEATURES,
        weightCol = WEIGHT_COLUMN,
        numTrees = NUM_TREES,
        maxDepth = MAX_DEPTH,
        maxBins = MAX_BINS
    )
    
    full_pipeline = Pipeline(stages=preprocessing_stages + [classifier])
    trained_model = full_pipeline.fit(training_set)
    
    trained_model.write().overwrite().save(MODEL_PATH) # warning: this will overwrite
    
    return trained_model, test_set


def evaluate_model(model, data):
    predictions = model.transform(data)
    
    auc_evaluator = BinaryClassificationEvaluator(
        labelCol = LABEL_COLUMN,
        rawPredictionCol = "rawPrediction",
        metricName = "areaUnderROC"
    )
    
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol = LABEL_COLUMN,
        predictionCol = "prediction",
        metricName = "f1"
    )
    
    auc_score = auc_evaluator.evaluate(predictions)
    f1_score = f1_evaluator.evaluate(predictions)
    
    print(f"AUC: {auc_score:.4f}")
    print(f"F1: {f1_score:.4f}")



def main():
    session = SparkSession.builder.appName(APP_NAME).getOrCreate()
    trained_model, test_set = train_model(session)
    evaluate_model(trained_model, test_set)
    session.stop()



if __name__ == "__main__":
    main()