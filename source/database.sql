CREATE DATABASE finance;
USE finance;

CREATE TABLE users (
    client_id BIGINT PRIMARY KEY,
    current_age INT,
    retirement_age INT,
    birth_year INT,
    birth_month INT,
    gender ENUM('Male','Female','Other'),
    address VARCHAR(255),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    per_capita_income DECIMAL(15,2),
    yearly_income DECIMAL(15,2),
    total_debt DECIMAL(15,2),
    credit_score INT,
    num_credit_cards INT
);

CREATE TABLE cards (
    card_id BIGINT PRIMARY KEY,
    client_id BIGINT NOT NULL,
    card_brand VARCHAR(50),
    card_type VARCHAR(20),
    card_number VARCHAR(20) UNIQUE,
    expires DATE,
    cvv CHAR(4),
    has_chip ENUM('Yes','No'),
    num_cards_issued INT,
    credit_limit DECIMAL(15,2),
    acct_open_date DATE,
    year_pin_last_changed YEAR,
    card_on_dark_web ENUM('Yes','No'),

    FOREIGN KEY (client_id) REFERENCES users(client_id)
);

CREATE TABLE mcc_codes (
    mcc BIGINT PRIMARY KEY,
    merchant_type VARCHAR(100) NOT NULL
);

CREATE TABLE transactions (
    transaction_id BIGINT PRIMARY KEY,
    trans_date DATETIME NOT NULL,
    client_id BIGINT NOT NULL,
    card_id BIGINT NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    use_chip VARCHAR(20) NOT NULL,
    merchant_id INT NOT NULL,
    mcc BIGINT NOT NULL,
    merchant_city VARCHAR(100),
    merchant_state VARCHAR(50),
    zip VARCHAR(10),
    errors VARCHAR(255),
    
    FOREIGN KEY (client_id) REFERENCES users(client_id),
    FOREIGN KEY (card_id) REFERENCES cards(card_id),
    FOREIGN KEY (mcc) REFERENCES mcc_codes(mcc)
);

-- Data Warehouse Schema for Fraud Detection Analytics
-- Star Schema: fact_transactions with dimensions

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE TABLE dim_users (
    user_key BIGINT PRIMARY KEY,
    client_id BIGINT NOT NULL,
    current_age INT,
    retirement_age INT,
    birth_year INT,
    birth_month INT,
    gender VARCHAR(10),
    address VARCHAR(255),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    per_capita_income DECIMAL(15,2),
    yearly_income DECIMAL(15,2),
    total_debt DECIMAL(15,2),
    credit_score INT,