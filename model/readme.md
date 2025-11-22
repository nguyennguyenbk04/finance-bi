# Online Payments Fraud Detection 

This project trains a fraud detection model on the [Rupak Roy Online Payments Fraud Detection dataset](https://www.kaggle.com/datasets/rupakroy/online-payments-fraud-detection-dataset) using PySpark and a Random Forest classifier.

---

## Requirements

- Python 3.10+  
- Apache Spark 4.x (Homebrew installation recommended)  
- PySpark, findspark, numpy (install in a virtual environment)  
- JDK 17+  

---

## Setup

1. **Create a Python virtual environment:**

```bash
cd model
python3 -m venv spark-venv-py310
source spark-venv-py310/bin/activate
```

2. **Install Python dependencies:**
```bash
pip install pyspark findspark numpy
```

3. **Install Spark (if needed):**
```bash
brew install apache-spark
```
## Running the training script

Remember to provide the `PS_20174392719_1491204439457_log.csv` file in the `/model` 

```
python train.py
```

## Running the predicting script

```
python predict.py PS_20174392719_1491204439457_log.csv
```