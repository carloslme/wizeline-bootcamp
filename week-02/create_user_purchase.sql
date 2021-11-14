CREATE TABLE user_purchase(
    invoice_number VARCHAR(10) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    detail VARCHAR(1000) NOT NULL,
    quantity INTEGER NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    unit_price NUMERIC (8, 3) NOT NULL,
    customer_id INTEGER NOT NULL,
    country VARCHAR(20) NOT NULL
):