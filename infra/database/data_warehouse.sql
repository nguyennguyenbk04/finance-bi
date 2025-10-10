CREATE DATABASE IF NOT EXISTS finance_dw;
USE finance_dw;

-- DIMENSIONS and FACTS for MySQL

CREATE TABLE dim_date (
    date_sk INT NOT NULL PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT,
    day_of_week INT,
    is_weekend TINYINT(1),
    INDEX idx_full_date (full_date),
    INDEX idx_year_month (year, month)
) ENGINE=InnoDB;

CREATE TABLE dim_user (
    client_id BIGINT PRIMARY KEY,       -- natural key from source
    user_sk BIGINT,                     -- surrogate (managed by ETL)
    birth_year INT,
    gender VARCHAR(10),
    yearly_income DECIMAL(15,2),
    total_debt DECIMAL(15,2),
    credit_score INT,
    num_credit_cards INT,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    user_segment VARCHAR(100),          -- Changed from customer_segment
    INDEX idx_user_sk (user_sk),        -- Changed from customer_sk
    INDEX idx_user_segment (user_segment) -- Changed from customer_segment
) ENGINE=InnoDB;

CREATE TABLE dim_card (
    card_id BIGINT PRIMARY KEY,         -- natural key from source
    card_sk BIGINT,                     -- surrogate (managed by ETL)
    client_id BIGINT,
    card_brand VARCHAR(50),
    card_type VARCHAR(20),
    credit_limit DECIMAL(15,2),
    acct_open_date DATE,
    has_chip ENUM('Yes','No'),
    card_on_dark_web ENUM('Yes','No'),
    INDEX idx_card_sk (card_sk),
    INDEX idx_client_id (client_id),
    INDEX idx_card_brand (card_brand),
    FOREIGN KEY (client_id) REFERENCES dim_user(client_id)  -- Changed from dim_customer
) ENGINE=InnoDB;

CREATE TABLE dim_merchant (
    merchant_id BIGINT PRIMARY KEY,     -- natural key from source
    merchant_sk BIGINT,                 -- surrogate (managed by ETL)
    mcc BIGINT,
    merchant_type VARCHAR(100),
    merchant_city VARCHAR(100),
    merchant_state VARCHAR(50),
    merchant_zip VARCHAR(10),
    INDEX idx_merchant_sk (merchant_sk),
    INDEX idx_mcc (mcc),
    INDEX idx_merchant_type (merchant_type)
) ENGINE=InnoDB;

CREATE TABLE dim_mcc (
    mcc BIGINT PRIMARY KEY,
    merchant_type VARCHAR(100),
    INDEX idx_merchant_type (merchant_type)
) ENGINE=InnoDB;

CREATE TABLE fact_transactions (
    transaction_id BIGINT PRIMARY KEY,  -- natural key from source
    date_sk INT NOT NULL,
    user_sk BIGINT,                     -- Changed from customer_sk
    card_sk BIGINT,
    merchant_sk BIGINT,
    mcc_sk BIGINT,                      -- Add MCC foreign key
    amount DECIMAL(15,2) NOT NULL,
    use_chip VARCHAR(20) NOT NULL,
    errors VARCHAR(255),
    is_fraud TINYINT(1) DEFAULT 0,
    fraud_label_source VARCHAR(50),
    transaction_count INT DEFAULT 1,
    INDEX idx_date_sk (date_sk),
    INDEX idx_user_sk (user_sk),        -- Changed from customer_sk
    INDEX idx_card_sk (card_sk),
    INDEX idx_merchant_sk (merchant_sk),
    INDEX idx_mcc_sk (mcc_sk),          -- Add MCC index
    INDEX idx_is_fraud (is_fraud),
    INDEX idx_date_fraud (date_sk, is_fraud),
    INDEX idx_mcc_fraud (mcc_sk, is_fraud),  -- Add MCC-fraud composite index
    FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),     -- Changed from customer_sk
    FOREIGN KEY (card_sk) REFERENCES dim_card(card_sk),
    FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk),
    FOREIGN KEY (mcc_sk) REFERENCES dim_mcc(mcc)  -- Add MCC foreign key
) ENGINE=InnoDB;