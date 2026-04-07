CREATE TABLE transactions_valid (
    user_id VARCHAR(50),
    amount FLOAT,
    timestamp TIMESTAMP,
    source VARCHAR(20)
);

CREATE TABLE transactions_invalid (
    user_id VARCHAR(50),
    amount FLOAT,
    timestamp TIMESTAMP,
    source VARCHAR(20)
);
