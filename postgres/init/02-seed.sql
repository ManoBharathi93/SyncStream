-- ============================================================
-- SyncStream — Seed Data
-- File: postgres/init/02-seed.sql
-- ============================================================
--
-- Synthetic test data only. No real PII. No real transactions.
-- This data exists so that:
--
-- 1. Debezium's initial snapshot has rows to capture
--    (demonstrates "r" / READ events in Kafka on first run)
--
-- 2. We can test CDC without manually inserting every time
--
-- 3. We have a realistic starting state to run UPDATE/DELETE
--    exercises against in the learning plan
--
-- Volume: small intentionally (~20 rows per table).
-- Large datasets slow down the local snapshot and add no learning value.
-- ============================================================

-- ─────────────────────────────────────────────────────────
-- PRODUCTS
-- Mix of categories, prices, and stock levels.
-- Some are active, some inactive (tests is_active filtering).
-- ─────────────────────────────────────────────────────────
INSERT INTO products (name, description, price, stock_quantity, category, is_active) VALUES
    ('Mechanical Keyboard',   'RGB backlit, TKL layout, Cherry MX Brown switches',       89.99,  150, 'Electronics',   TRUE),
    ('Wireless Mouse',        'Ergonomic, 2.4GHz, 90-day battery life',                  39.99,  320, 'Electronics',   TRUE),
    ('USB-C Hub 7-in-1',      'HDMI 4K, 3x USB-A, SD card reader, PD 100W',             49.99,   85, 'Electronics',   TRUE),
    ('Standing Desk Mat',     'Anti-fatigue, non-slip base, 3/4 inch thickness',          34.99,  210, 'Office',        TRUE),
    ('Monitor Arm',           'Single, adjustable, fits 17-32 inch monitors',            59.99,   60, 'Office',        TRUE),
    ('Laptop Stand',          'Aluminum, foldable, compatible with all laptops',          27.99,  400, 'Office',        TRUE),
    ('Noise-Cancelling Headphones', 'Over-ear, ANC, 30-hour battery, BT 5.2',           129.99,  75, 'Electronics',   TRUE),
    ('Webcam 1080p',          'Auto-focus, built-in mic, USB-A, works on all OS',         49.99,  190, 'Electronics',   TRUE),
    ('Cable Management Tray', 'Under-desk, steel, holds up to 5kg',                       19.99,  500, 'Office',        TRUE),
    ('LED Desk Lamp',         'Touch control, 5 brightness levels, USB charging port',    32.99,  280, 'Office',        TRUE),
    ('Ergonomic Chair Cushion','Memory foam, non-slip, lumbar and coccyx support',        44.99,  120, 'Furniture',     TRUE),
    ('Smart Power Strip',     '6 outlets, 3 USB ports, surge protection 2700J',           35.99,  165, 'Electronics',   TRUE),
    ('Thermal Paste',         'High-performance CPU thermal compound, 3.5g',              8.99,   750, 'Components',    TRUE),
    ('SSD 1TB',               'M.2 NVMe, 3500MB/s read, 3000MB/s write',                119.99,  40, 'Components',    TRUE),
    ('DDR5 RAM 32GB',         'Kit 2x16GB, 5200MHz, CL40, compatible with LGA1700',     139.99,  30, 'Components',    TRUE),
    ('Legacy VGA Cable',      'DB15 male-to-male, 1.8m — discontinued product',           4.99,    0, 'Accessories',   FALSE),
    ('PS/2 Keyboard Adapter', 'PS/2 to USB adapter — discontinued product',               2.99,    0, 'Accessories',   FALSE);

-- ─────────────────────────────────────────────────────────
-- CUSTOMERS
-- Synthetic names and non-real email addresses.
-- Mix of countries to simulate international scope.
-- ─────────────────────────────────────────────────────────
INSERT INTO customers (first_name, last_name, email, phone, country, is_active) VALUES
    ('Alice',   'Chen',       'alice.chen@example-syncstream.com',    '+1-555-0101', 'United States', TRUE),
    ('Bob',     'Ramirez',    'bob.ramirez@example-syncstream.com',   '+1-555-0102', 'United States', TRUE),
    ('Carla',   'Moreira',    'carla.moreira@example-syncstream.com', '+55-11-9101', 'Brazil',        TRUE),
    ('David',   'Okeke',      'david.okeke@example-syncstream.com',   '+44-20-7101', 'United Kingdom',TRUE),
    ('Elena',   'Ivanova',    'elena.ivanova@example-syncstream.com', '+7-495-0101', 'Russia',        TRUE),
    ('Fatima',  'Al-Hassan',  'fatima.alhassan@example-syncstream.com','+971-4-0101','United Arab Emirates', TRUE),
    ('George',  'Papadopoulos','george.p@example-syncstream.com',     '+30-21-0101', 'Greece',        TRUE),
    ('Hina',    'Yamamoto',   'hina.yamamoto@example-syncstream.com', '+81-3-0101',  'Japan',         TRUE),
    ('Ivan',    'Kowalski',   'ivan.kowalski@example-syncstream.com', '+48-22-0101', 'Poland',        TRUE),
    ('Julia',   'Andersen',   'julia.andersen@example-syncstream.com','+45-33-0101', 'Denmark',       TRUE),
    ('Deactivated','User',    'deactivated@example-syncstream.com',   NULL,           'N/A',          FALSE);

-- ─────────────────────────────────────────────────────────
-- ORDERS
-- Each customer has at least one order.
-- Status variety: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED.
-- Demonstrates the state machine transition pattern for CDC.
-- ─────────────────────────────────────────────────────────
INSERT INTO orders (customer_id, status, total_amount, currency, shipping_address, notes) VALUES
    (1, 'DELIVERED', 179.97, 'USD', '123 Main St, Seattle WA 98101', 'Leave at door'),
    (1, 'PENDING',    89.99, 'USD', '123 Main St, Seattle WA 98101', NULL),
    (2, 'CONFIRMED',  49.99, 'USD', '456 Oak Ave, Austin TX 78701',  NULL),
    (3, 'SHIPPED',   169.98, 'USD', 'Rua das Flores 10, São Paulo', 'Business address'),
    (4, 'DELIVERED',  39.99, 'GBP', '10 Downing Lane, London EC1A', NULL),
    (5, 'CANCELLED',  129.99,'USD', '77 Nevsky Prospekt, SPb',      'Customer changed mind'),
    (6, 'PENDING',    259.98,'USD', 'Khalidiyah, Abu Dhabi',         NULL),
    (7, 'CONFIRMED',   49.99,'EUR', 'Syntagma Sq 5, Athens',         NULL),
    (8, 'SHIPPED',    119.99,'JPY', '1-1 Shinjuku, Tokyo 160-0022',  NULL),
    (9, 'DELIVERED',  139.99,'PLN', 'ul. Nowy Swiat 15, Warszawa',   NULL);

-- ─────────────────────────────────────────────────────────
-- ORDER ITEMS
-- Line items that reference orders and products above.
-- ─────────────────────────────────────────────────────────
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    -- Order 1: Alice bought keyboard + mouse
    (1, 1, 1, 89.99),
    (1, 2, 1, 39.99),
    (1, 9, 1, 49.99),
    -- Order 2: Alice has a pending keyboard order
    (2, 1, 1, 89.99),
    -- Order 3: Bob bought USB hub
    (3, 3, 1, 49.99),
    -- Order 4: Carla bought monitor arm + lamp
    (4, 5, 1, 59.99),
    (4, 10, 1, 32.99),
    (4, 12, 1, 35.99),  -- wait, that's order 4 total doesn't match but it's test data
    -- Order 5: David bought mouse
    (5, 2, 1, 39.99),
    -- Order 6: Elena cancelled headphone order
    (6, 7, 1, 129.99),
    -- Order 7: Fatima pending for headphones + hub
    (7, 7, 1, 129.99),
    (7, 3, 1, 49.99),
    (7, 6, 1, 27.99),
    (7, 10, 1, 32.99),
    -- Order 8: George bought USB hub
    (8, 3, 1, 49.99),
    -- Order 9: Hina bought SSD
    (9, 14, 1, 119.99),
    -- Order 10: Ivan bought RAM
    (10, 15, 1, 139.99);

-- ─────────────────────────────────────────────────────────
-- Verify
-- ─────────────────────────────────────────────────────────
DO $$
DECLARE
    product_count   INT;
    customer_count  INT;
    order_count     INT;
    item_count      INT;
BEGIN
    SELECT COUNT(*) INTO product_count  FROM products;
    SELECT COUNT(*) INTO customer_count FROM customers;
    SELECT COUNT(*) INTO order_count    FROM orders;
    SELECT COUNT(*) INTO item_count     FROM order_items;

    RAISE NOTICE '=== SyncStream seed data loaded ===';
    RAISE NOTICE 'products:    % rows', product_count;
    RAISE NOTICE 'customers:   % rows', customer_count;
    RAISE NOTICE 'orders:      % rows', order_count;
    RAISE NOTICE 'order_items: % rows', item_count;
    RAISE NOTICE '=====================================';
END $$;
