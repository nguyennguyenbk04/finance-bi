"""
Finance Analytics Automation DAG
Automates the complete finance analytics workflow from infrastructure setup to batch processing
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import os
import subprocess
import logging

# Default arguments
default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'finance_analytics_workflow',
    default_args=default_args,
    description='Complete finance analytics workflow automation',
    schedule=None,  # Manual trigger; change to '@daily' for scheduled runs
    catchup=False,
    tags=['finance', 'automation', 'batch'],
)

def check_infrastructure_health():
    """Check if all required services are running"""
    try:
        import requests
        import socket
        
        # Check services by trying to connect to them
        services = {
            'Kafka': ('kafka', 9092),
            'MySQL': ('mysql', 3306),
            'MinIO': ('minio', 9000)
        }
        
        for service_name, (host, port) in services.items():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result != 0:
                    raise Exception(f"{service_name} is not reachable at {host}:{port}")
                
                logging.info(f"{service_name} is running at {host}:{port}")
            except Exception as e:
                logging.error(f"Failed to connect to {service_name}: {e}")
                raise

        logging.info("All infrastructure services are running")
        return True

    except Exception as e:
        logging.error(f"Infrastructure health check failed: {e}")
        raise

def initialize_kafka_topics():
    """Initialize Kafka topics by inserting/deleting dummy records in MySQL"""
    try:
        import mysql.connector
        from time import sleep

        # Connect to MySQL (use container name from Airflow)
        conn = mysql.connector.connect(
            host='mysql',
            port=3306,
            user='root',
            password='root123',
            database='finance'
        )
        cursor = conn.cursor()

        # Clean up any existing dummy records first (in reverse order due to foreign keys)
        logging.info("Cleaning up any existing dummy records...")
        cursor.execute("DELETE FROM transactions WHERE transaction_id = 999999999999")
        cursor.execute("DELETE FROM cards WHERE card_id = 999999999999")
        cursor.execute("DELETE FROM mcc_codes WHERE mcc = 999999999999")
        cursor.execute("DELETE FROM users WHERE client_id = 999999999999")
        conn.commit()

        sleep (2)

        # Insert dummy records to initialize topics
        logging.info("Inserting dummy records to initialize Kafka topics...")
        
        cursor.execute("""
            INSERT INTO users (client_id, current_age, retirement_age, birth_year, birth_month, gender, address, latitude, longitude, per_capita_income, yearly_income, total_debt, credit_score, num_credit_cards) 
            VALUES (999999999999, 30, 65, 1993, 1, 'Other', 'Dummy Address', 0.000000, 0.000000, 0.00, 0.00, 0.00, 500, 0)
        """)
        
        cursor.execute("""
            INSERT INTO mcc_codes (mcc, merchant_type) 
            VALUES (999999999999, 'Dummy Merchant Type')
        """)
        
        cursor.execute("""
            INSERT INTO cards (card_id, client_id, card_brand, card_type, card_number, expires, cvv, has_chip, num_cards_issued, credit_limit, acct_open_date, year_pin_last_changed, card_on_dark_web) 
            VALUES (999999999999, 999999999999, 'Dummy', 'Dummy', '9999999999999999', '2099-12-31', '999', 'No', 0, 0.00, '2000-01-01', 2000, 'No')
        """)
        
        cursor.execute("""
            INSERT INTO transactions (transaction_id, trans_date, client_id, card_id, amount, use_chip, merchant_id, mcc, merchant_city, merchant_state, zip, errors) 
            VALUES (999999999999, '2000-01-01 00:00:00', 999999999999, 999999999999, 0.00, 'Dummy Transaction', 0, 999999999999, 'Dummy City', 'XX', '00000', 'Dummy record for topic initialization')
        """)

        conn.commit()
        logging.info("Dummy records inserted successfully")

        # Wait for CDC to process
        logging.info("Waiting for CDC to process changes...")
        sleep(5)

        # Delete dummy records (in reverse order due to foreign keys)
        logging.info("Cleaning up dummy records...")
        cursor.execute("DELETE FROM transactions WHERE transaction_id = 999999999999")
        cursor.execute("DELETE FROM cards WHERE card_id = 999999999999")
        cursor.execute("DELETE FROM mcc_codes WHERE mcc = 999999999999")
        cursor.execute("DELETE FROM users WHERE client_id = 999999999999")

        conn.commit()
        
        sleep(5)

        cursor.close()
        conn.close()

        logging.info("Kafka topics initialized successfully")
        return True

    except Exception as e:
        logging.error(f"Kafka initialization failed: {e}")
        raise

def check_kafka_topics():
    """Check if Kafka topics are healthy"""
    import subprocess
    try:
        # Check if Kafka topics exist
        result = subprocess.run([
            'docker', 'exec', 'kafka', 
            '/kafka/bin/kafka-topics.sh', 
            '--bootstrap-server', 'kafka:9092', 
            '--list'
        ], capture_output=True, text=True, check=True)
        
        topics = result.stdout.strip().split('\n')
        expected_topics = [
            'finance.finance.transactions',
            'finance.finance.users', 
            'finance.finance.cards',
            'finance.finance.mcc_codes',
            'finance.finance.fraud_labels'
        ]
        
        missing_topics = [topic for topic in expected_topics if topic not in topics]
        
        if missing_topics:
            raise Exception(f"Missing Kafka topics: {missing_topics}")
            
        logging.info(f"All Kafka topics are healthy: {expected_topics}")
        return True
        
    except Exception as e:
        logging.error(f"Kafka health check failed: {e}")
        raise

def run_jupyter_notebook(notebook_path, execution_count_file=None):
    """Execute a Jupyter notebook and track execution"""
    try:
        # Check if already executed (for one-time tasks)
        if execution_count_file and os.path.exists(execution_count_file):
            logging.info(f"Notebook {notebook_path} already executed, skipping...")
            return True

        logging.info(f"Executing notebook: {notebook_path}")

        # Check if notebook exists
        if not os.path.exists(notebook_path):
            raise Exception(f"Notebook not found: {notebook_path}")

        # Use nbconvert to execute the notebook
        import nbformat
        from nbconvert.preprocessors import ExecutePreprocessor
        
        with open(notebook_path) as f:
            nb = nbformat.read(f, as_version=4)
        
        ep = ExecutePreprocessor(timeout=1800, kernel_name='python3')
        ep.preprocess(nb, {'metadata': {'path': os.path.dirname(notebook_path)}})
        
        # Save the executed notebook
        with open(notebook_path, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)

        # Mark as executed for one-time tasks
        if execution_count_file:
            os.makedirs(os.path.dirname(execution_count_file), exist_ok=True)
            with open(execution_count_file, 'w') as f:
                f.write('executed')

        logging.info(f"Notebook {notebook_path} executed successfully")
        return True

    except Exception as e:
        logging.error(f"Notebook execution failed: {e}")
        raise

# Task 1: Check infrastructure health
health_check = PythonOperator(
    task_id='check_infrastructure_health',
    python_callable=check_infrastructure_health,
    dag=dag,
)

# Task 2: Initialize Kafka topics
init_kafka = PythonOperator(
    task_id='initialize_kafka_topics',
    python_callable=initialize_kafka_topics,
    op_kwargs={},
    trigger_rule='all_success',
    # Sleep 10 seconds then check Kafka topics
    on_success_callback=lambda context: (
        __import__('time').sleep(10),
        check_kafka_topics()
    ),
    dag=dag,
)

# Task 3: Run preprocessing.ipynb
preprocessing = PythonOperator(
    task_id='run_preprocessing',
    python_callable=lambda: run_jupyter_notebook(
        '/opt/airflow/project_root/src/batch_processing/preprocessing.ipynb'
    ),
    dag=dag,
)

# Task 4: Run database_migration.ipynb
migration = PythonOperator(
    task_id='run_database_migration',
    python_callable=lambda: run_jupyter_notebook(
        '/opt/airflow/project_root/src/batch_processing/database_migration.ipynb'
    ),
    dag=dag,
)

# Task 5: Run dw_population.ipynb (for data updates)
dw_population = PythonOperator(
    task_id='run_dw_population',
    python_callable=lambda: run_jupyter_notebook(
        '/opt/airflow/project_root/src/batch_processing/dw_population.ipynb'
    ),
    dag=dag,
)

# Task 6: Final status check
final_check = BashOperator(
    task_id='final_status_check',
    bash_command='''
    echo "Finance Analytics Workflow Completed Successfully"
    echo "Timestamp: $(date)"
    echo "Infrastructure: Running"
    echo "Kafka Topics: Initialized"
    echo "Preprocessing: Completed"
    echo "Migration: Completed"
    echo "DW Population: Completed"
    ''',
    dag=dag,
)

# Define task dependencies (workflow sequence)
health_check >> init_kafka >> preprocessing >> migration >> dw_population >> final_check