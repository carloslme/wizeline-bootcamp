CREATE SCHEMA IF NOT EXISTS postgres;

CREATE TABLE IF NOT EXISTS postgres.user_purchase(
    invoice_number VARCHAR(10),
    stock_code VARCHAR(20),
    detail VARCHAR(1000),
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMERIC (8, 3),
    customer_id INTEGER,
    country VARCHAR(20) L
);