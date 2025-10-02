DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_users CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_payment_methods CASCADE;
DROP TABLE IF EXISTS dim_shipping_methods CASCADE;

CREATE TABLE dim_users (
    user_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    address TEXT,
    phone_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    category VARCHAR(100),
    currency VARCHAR(10) DEFAULT 'USD',
    price NUMERIC(10,2) NOT NULL,
    cost NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    method_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_shipping_methods (
    shipping_method_id SERIAL PRIMARY KEY,
    method_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    payment_method_id INT NOT NULL,
    shipping_method_id INT NOT NULL,
    quantity INT NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    CONSTRAINT fk_payment FOREIGN KEY (payment_method_id) REFERENCES dim_payment_methods(payment_method_id),
    CONSTRAINT fk_shipping FOREIGN KEY (shipping_method_id) REFERENCES dim_shipping_methods(shipping_method_id),
    CONSTRAINT chk_quantity CHECK (quantity > 0)
);
