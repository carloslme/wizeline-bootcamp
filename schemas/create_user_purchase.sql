CREATE SCHEMA IF NOT EXISTS postgres;

CREATE TABLE IF NOT EXISTS postgres.user_purchase(
    invoice_number VARCHAR(10) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    detail VARCHAR(1000) NOT NULL,
    quantity INTEGER NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    unit_price NUMERIC (8, 3) NOT NULL,
    customer_id INTEGER NOT NULL,
    country VARCHAR(20) NOT NULL
);

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    invoice_number DROP NOT NULL;

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    stock_code DROP NOT NULL;

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    detail DROP NOT NULL;

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    quantity DROP NOT NULL;

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    invoice_date DROP NOT NULL;

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    unit_price DROP NOT NULL;

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    customer_id DROP NOT NULL;

ALTER TABLE
    postgres.user_purchase
ALTER COLUMN
    country DROP NOT NULL;