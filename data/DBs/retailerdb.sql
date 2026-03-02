CREATE SCHEMA retailer;

DROP TABLE IF EXISTS retailer.products;
CREATE TABLE retailer.products (
    product_id INT PRIMARY KEY,
    name VARCHAR(255),
    category_id INT,
    price DECIMAL(10,2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS retailer.categories;
CREATE TABLE retailer.categories (
    category_id INT PRIMARY KEY,
    name VARCHAR(255),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS retailer.customers;
CREATE TABLE retailer.customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS retailer.orders;
CREATE TABLE retailer.orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS retailer.order_items;
CREATE TABLE retailer.order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10,2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- 1) Reusable function (put it in public so it can be reused everywhere)
CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2) Triggers (one per table)

DROP TRIGGER IF EXISTS trg_products_updated_at ON retailer.products;
CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON retailer.products
FOR EACH ROW
EXECUTE FUNCTION public.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_categories_updated_at ON retailer.categories;
CREATE TRIGGER trg_categories_updated_at
BEFORE UPDATE ON retailer.categories
FOR EACH ROW
EXECUTE FUNCTION public.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_customers_updated_at ON retailer.customers;
CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON retailer.customers
FOR EACH ROW
EXECUTE FUNCTION public.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_orders_updated_at ON retailer.orders;
CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON retailer.orders
FOR EACH ROW
EXECUTE FUNCTION public.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_order_items_updated_at ON retailer.order_items;
CREATE TRIGGER trg_order_items_updated_at
BEFORE UPDATE ON retailer.order_items
FOR EACH ROW
EXECUTE FUNCTION public.update_updated_at_column();


-- Insert into categories\
INSERT INTO retailer.categories (category_id, name) VALUES
    (1, 'Electronics'),
    (2, 'Clothing'),
    (3, 'Home Appliances'),
    (4, 'Books'),
    (5, 'Toys'),
    (6, 'Furniture'),
    (7, 'Sports'),
    (8, 'Health & Beauty'),
    (9, 'Automotive'),
    (10, 'Grocery');

-- Insert into products
INSERT INTO retailer.products (product_id, name, category_id, price) VALUES
    (101, 'Laptop', 1, 1000.00),
    (102, 'T-Shirt', 2, 20.00),
    (103, 'Refrigerator', 3, 500.00),
    (104, 'Novel', 4, 15.00),
    (105, 'Doll', 5, 25.00),
    (106, 'Sofa', 6, 300.00),
    (107, 'Football', 7, 30.00),
    (108, 'Shampoo', 8, 10.00),
    (109, 'Car Tire', 9, 100.00),
    (110, 'Rice Bag', 10, 40.00),
    (111, 'Headphones', 1, 150.00),
    (112, 'Jeans', 2, 50.00),
    (113, 'Microwave', 3, 200.00),
    (114, 'Comic Book', 4, 12.00),
    (115, 'Toy Car', 5, 35.00),
    (116, 'Dining Table', 6, 450.00),
    (117, 'Basketball', 7, 25.00),
    (118, 'Perfume', 8, 60.00),
    (119, 'Engine Oil', 9, 45.00),
    (120, 'Coffee Beans', 10, 20.00);

-- Insert into customers
INSERT INTO retailer.customers (customer_id, name, email) VALUES
    (1, 'John Doe', 'john@example.com'),
    (2, 'Jane Smith', 'jane@example.com'),
    (3, 'Michael Johnson', 'michael@example.com'),
    (4, 'Emily Davis', 'emily@example.com'),
    (5, 'David Wilson', 'david@example.com'),
    (6, 'Sarah Miller', 'sarah@example.com'),
    (7, 'Daniel Anderson', 'daniel@example.com'),
    (8, 'Olivia Thomas', 'olivia@example.com'),
    (9, 'James Martinez', 'james@example.com'),
    (10, 'Emma Hernandez', 'emma@example.com');

-- Insert into orders
INSERT INTO retailer.orders (order_id, customer_id, total_amount) VALUES
    (1001, 1, 1020.00),
    (1002, 2, 20.00),
    (1003, 3, 150.00),
    (1004, 4, 35.00),
    (1005, 5, 60.00),
    (1006, 6, 450.00),
    (1007, 7, 200.00),
    (1008, 8, 75.00),
    (1009, 9, 120.00),
    (1010, 10, 90.00);

-- Insert into order_items
INSERT INTO retailer.order_items (order_item_id, order_id, product_id, quantity, price) VALUES
    (1, 1001, 101, 1, 1000.00),
    (2, 1001, 102, 1, 20.00),
    (3, 1002, 102, 1, 20.00),
    (4, 1003, 111, 1, 150.00),
    (5, 1004, 115, 1, 35.00),
    (6, 1005, 118, 1, 60.00),
    (7, 1006, 116, 1, 450.00),
    (8, 1007, 113, 1, 200.00),
    (9, 1008, 120, 1, 75.00),
    (10, 1009, 119, 1, 120.00);
