CREATE TABLE user_behavior_metric(
    customer INTEGER,
    amount_spent DECIMAL(18, 5),
    review_score INTEGER,
    review_count INTEGER,
    insert_date DATE
);

CREATE TABLE IF NOT EXISTS postgres.user_behavior_metric(
    customer INTEGER NOT NULL,
    amount_spent DECIMAL(18, 5) NOT NULL,
    review_score INTEGER NOT NULL,
    review_count INTEGER NOT NULL,
    insert_date TIMESTAMP NOT NULL
);