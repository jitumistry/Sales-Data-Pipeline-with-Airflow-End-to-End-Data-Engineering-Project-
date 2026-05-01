-- Dimension table
CREATE TABLE IF NOT EXISTS dim_customers (
    surrogate_key SERIAL PRIMARY KEY,
    customer_id INT,
    name TEXT,
    email TEXT,
    city TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current CHAR(1)
);

-- Fact table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_amount NUMERIC(10,2),
    order_date TIMESTAMP
);

-- Metadata table
CREATE TABLE IF NOT EXISTS etl_metadata (
    pipeline_name TEXT PRIMARY KEY,
    last_run TIMESTAMP
);

-- Initial value

DROP TABLE IF EXISTS etl_metadata;

CREATE TABLE etl_metadata (
    table_name TEXT PRIMARY KEY,
    last_run TIMESTAMP
);

INSERT INTO etl_metadata (table_name, last_run)
VALUES ('fact_orders', '1900-01-01');

