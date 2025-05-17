-- Users table
CREATE TABLE users (
    id STRING(36) NOT NULL,
    name STRING(255) NOT NULL,
    email STRING(255) NOT NULL,
    status STRING(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
) PRIMARY KEY(id);

-- Orders table with foreign key to users
CREATE TABLE orders (
    id STRING(36) NOT NULL,
    user_id STRING(36) NOT NULL,
    order_status STRING(20) NOT NULL,
    total_amount NUMERIC NOT NULL,
    items_count INT64 NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT FK_orders_user_id FOREIGN KEY (user_id) REFERENCES users(id)
) PRIMARY KEY(id);

-- Indexes for better performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_orders_created_at ON orders(created_at); 