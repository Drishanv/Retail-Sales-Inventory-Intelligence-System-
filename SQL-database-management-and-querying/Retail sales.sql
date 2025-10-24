-- Phase 2 SQL – Database Management and Querying

-- Create and use database

CREATE DATABASE retail;

USE retail;

-- Create tables 

SET FOREIGN_KEY_CHECKS = 0;

-- 1) brands
CREATE TABLE IF NOT EXISTS brands (
  brand_id INT PRIMARY KEY,
  brand_name VARCHAR(100) NOT NULL
);

-- 2) categories
CREATE TABLE IF NOT EXISTS categories (
  category_id INT PRIMARY KEY,
  category_name VARCHAR(100) NOT NULL
);

-- 3) products
CREATE TABLE IF NOT EXISTS products (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(200) NOT NULL,
  brand_id INT NOT NULL,
  category_id INT NOT NULL,
  model_year INT NOT NULL,
  list_price DECIMAL(10,2) NOT NULL,
  FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
  FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- 4) stores
CREATE TABLE IF NOT EXISTS stores (
  store_id INT PRIMARY KEY,
  store_name VARCHAR(150) NOT NULL,
  phone VARCHAR(30),
  email VARCHAR(150),
  street VARCHAR(200),
  city VARCHAR(100),
  state VARCHAR(50),
  zip_code VARCHAR(20)
);

-- 5) staffs
CREATE TABLE IF NOT EXISTS staffs (
  staff_id INT PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  email VARCHAR(150),
  phone VARCHAR(30),
  active TINYINT(1) NOT NULL,
  store_id INT NOT NULL,
  manager_id INT NULL,
  FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

ALTER TABLE staffs DROP FOREIGN KEY staffs_ibfk_2;


-- 6) customers
CREATE TABLE IF NOT EXISTS customers (
  customer_id INT PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  phone VARCHAR(30),
  email VARCHAR(150),
  street VARCHAR(200),
  city VARCHAR(100),
  state VARCHAR(50),
  zip_code VARCHAR(20)
);

-- 7) orders
CREATE TABLE IF NOT EXISTS orders (
  order_id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  order_status INT NOT NULL,
  order_date DATE NOT NULL,
  required_date DATE,
  shipped_date DATE,
  store_id INT NOT NULL,
  staff_id INT NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  FOREIGN KEY (store_id) REFERENCES stores(store_id),
  FOREIGN KEY (staff_id) REFERENCES staffs(staff_id)
);

-- Load data in orders table

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Orders_iso.csv'
INTO TABLE orders
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES
(@order_id,@customer_id,@order_status,@order_date,@required_date,@shipped_date,@store_id,@staff_id)
SET
  order_id      = CAST(@order_id AS UNSIGNED),
  customer_id   = CAST(@customer_id AS UNSIGNED),
  order_status  = CAST(@order_status AS UNSIGNED),
  order_date    = CASE WHEN @order_date = ''    THEN NULL
                       ELSE COALESCE(STR_TO_DATE(@order_date, '%Y-%m-%d'),
                                     STR_TO_DATE(@order_date, '%Y-%m-%d %H:%i:%s')) END,
  required_date = CASE WHEN @required_date = '' THEN NULL
                       ELSE COALESCE(STR_TO_DATE(@required_date, '%Y-%m-%d'),
                                     STR_TO_DATE(@required_date, '%Y-%m-%d %H:%i:%s')) END,
  shipped_date  = CASE WHEN @shipped_date IN ('','0000-00-00','0000-00-00 00:00:00') THEN NULL
                       ELSE COALESCE(STR_TO_DATE(@shipped_date, '%Y-%m-%d'),
                                     STR_TO_DATE(@shipped_date, '%Y-%m-%d %H:%i:%s')) END,
  store_id      = CAST(@store_id AS UNSIGNED),
  staff_id      = CAST(@staff_id AS UNSIGNED);


-- 8) order_items
CREATE TABLE IF NOT EXISTS order_items (
  order_id INT NOT NULL,
  item_id INT NOT NULL,
  product_id INT NOT NULL,
  quantity INT NOT NULL,
  list_price DECIMAL(10,2) NOT NULL,
  discount DECIMAL(5,2) DEFAULT 0,
  PRIMARY KEY (order_id, item_id),
  FOREIGN KEY (order_id) REFERENCES orders(order_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);

ALTER TABLE order_items DROP FOREIGN KEY order_items_ibfk_1; 
ALTER TABLE order_items DROP FOREIGN KEY order_items_ibfk_2;

-- Load data in order items table

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Order_Items_clean.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES;

-- 9) stocks
CREATE TABLE IF NOT EXISTS stocks (
  store_id INT NOT NULL,
  product_id INT NOT NULL,
  quantity INT NOT NULL,
  PRIMARY KEY (store_id, product_id),
  FOREIGN KEY (store_id) REFERENCES stores(store_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);

SET FOREIGN_KEY_CHECKS = 1;

-- Analysis part

-- 1. Store-wise & Region-wise sales analysis

-- a. Store wise totals
SELECT
  s.store_id,
  s.store_name,
  s.city,
  s.state,
  s.zip_code,
  SUM(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS net_sales,
  COUNT(DISTINCT o.order_id) AS orders_count,
  SUM(oi.quantity) AS units_sold
FROM order_items oi
JOIN orders o  ON o.order_id = oi.order_id
JOIN stores s  ON s.store_id = o.store_id
GROUP BY s.store_id, s.store_name, s.city, s.state, s.zip_code
ORDER BY net_sales DESC;

-- b. Monthly trend by store & state (region proxy)

SELECT
  DATE_FORMAT(o.order_date, '%Y-%m') AS month,
  s.state                             AS region,
  s.store_name,
  SUM(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS net_sales
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
JOIN stores s ON s.store_id = o.store_id
GROUP BY DATE_FORMAT(o.order_date, '%Y-%m'), s.state, s.store_name
ORDER BY month, region, net_sales DESC; 

-- 2. Product-wise sales & inventory trends

-- a. Sales by product (with brand and category)

SELECT
  p.product_id,
  p.product_name,
  b.brand_name,
  c.category_name,
  SUM(oi.quantity) AS units_sold,
  SUM(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS net_sales
FROM order_items oi
JOIN products p   ON p.product_id = oi.product_id
JOIN brands b     ON b.brand_id   = p.brand_id
JOIN categories c ON c.category_id= p.category_id
GROUP BY p.product_id, p.product_name, b.brand_name, c.category_name
ORDER BY net_sales DESC;

-- b. Current inventory on hand (by store & product)

SELECT
  s.store_name,
  p.product_name,
  st.quantity AS on_hand
FROM stocks st
JOIN stores s  ON s.store_id = st.store_id
JOIN products p ON p.product_id = st.product_id
ORDER BY s.store_name, p.product_name;

-- 3. Staff performance reports

SELECT
  st.staff_id,
  st.first_name,
  st.last_name,
  s.store_name,
  COUNT(DISTINCT o.order_id) AS orders_handled,
  SUM(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS net_sales,
  AVG(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS avg_order_value,
  MIN(o.order_date) AS first_order_date,
  MAX(o.order_date) AS last_order_date
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
JOIN staffs st      ON st.staff_id = o.staff_id
JOIN stores s       ON s.store_id  = o.store_id
GROUP BY st.staff_id, st.first_name, st.last_name, s.store_name
ORDER BY net_sales DESC;

-- 4. Customer orders and order frequency

-- a. overall customer metrics

SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  COUNT(DISTINCT o.order_id) AS order_count,
  SUM(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS total_spend,
  MIN(o.order_date) AS first_order_date,
  MAX(o.order_date) AS last_order_date
FROM customers c
JOIN orders o       ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id   = o.order_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spend DESC;

-- 5. Revenue and discount analysis

-- a. Monthly revenue (gross vs net) & avg discount rate

SELECT
  DATE_FORMAT(o.order_date, '%Y-%m') AS month,
  AVG(COALESCE(oi.discount,0)) AS avg_discount_rate,
  SUM(oi.quantity * oi.list_price) AS gross_revenue,
  SUM(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS net_revenue,
  SUM(oi.quantity * oi.list_price * COALESCE(oi.discount,0))       AS discount_value
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
GROUP BY DATE_FORMAT(o.order_date, '%Y-%m')
ORDER BY month;

-- b. Top discounted products

SELECT
  p.product_id,
  p.product_name,
  SUM(oi.quantity * oi.list_price * COALESCE(oi.discount,0)) AS total_discount_value,
  SUM(oi.quantity * oi.list_price * (1 - COALESCE(oi.discount,0))) AS net_sales
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_discount_value DESC
LIMIT 20;