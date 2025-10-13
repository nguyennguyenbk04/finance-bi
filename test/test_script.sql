USE finance;

INSERT INTO users (client_id, current_age, retirement_age, birth_year, birth_month, gender, address, latitude, longitude, per_capita_income, yearly_income, total_debt, credit_score, num_credit_cards)
VALUES (2000, 30, 65, 1994, 5, 'Male', 'Test Address', 40.7128, -74.0060, 50000.00, 60000.00, 10000.00, 700, 2);

INSERT INTO cards (card_id, client_id, card_brand, card_type)
VALUES (6146, 2000, 'Visa', 'Credit');

INSERT INTO mcc_codes (mcc, merchant_type)
VALUES (9403, 'Test Merchant Type');

INSERT INTO transactions (transaction_id, trans_date, client_id, card_id, amount, use_chip, merchant_id, mcc, merchant_city, merchant_state, zip, errors)
VALUES (23761875, '2025-10-01 23:57:00', 2000, 6146, 100.00, 'Yes', 12345, 9403, 'Test City', 'TX', '12345', NULL);


DELETE FROM transactions WHERE transaction_id = 23761875;
DELETE FROM cards WHERE card_id = 6146;
DELETE FROM mcc_codes WHERE mcc = 9403;
DELETE FROM users WHERE client_id = 2000;