CREATE TABLE IF NOT EXISTS users (
  user_id SERIAL PRIMARY KEY,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(100) NOT NULL,
  created_at TIMESTAMP DEFAULT NOW ()
);

CREATE TABLE IF NOT EXISTS products (
  product_id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  order_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users (user_id),
  product_id INTEGER REFERENCES products (product_id),
  quantity INTEGER NOT NULL,
  order_date TIMESTAMP DEFAULT NOW ()
);

CREATE INDEX idx_orders_user_id ON orders (user_id);

CREATE INDEX idx_orders_order_date ON orders (order_date);
