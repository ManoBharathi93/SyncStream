-- ============================================================
-- SyncStream — Database Schema
-- File: postgres/init/01-schema.sql
-- ============================================================
--
-- This file runs automatically when the PostgreSQL container
-- starts for the first time (via docker-entrypoint-initdb.d).
--
-- WHY REPLICA IDENTITY FULL?
--
-- By default, PostgreSQL only includes the primary key in the
-- WAL's "old tuple" (the before-image of an UPDATE or DELETE).
-- This means Debezium's "before" field would only contain {id: 1},
-- not the full row {id: 1, name: "...", price: ...}.
--
-- REPLICA IDENTITY FULL tells PostgreSQL to log the ENTIRE old row.
-- This is required so consumers can:
--   - Know what changed (before vs after comparison)
--   - Handle DELETEs that need the full row for downstream removal
--   - Support audit trail use cases
--
-- Trade-off: REPLICA IDENTITY FULL slightly increases WAL size
-- because full rows are logged. For most tables this is negligible.
-- For tables with large BYTEA/TEXT columns, this can be significant.
-- ============================================================

-- ─────────────────────────────────────────────────────────
-- PRODUCTS TABLE
-- Simulates an e-commerce product catalog.
-- Primary CDC demonstration table.
-- ─────────────────────────────────────────────────────────
CREATE TABLE products (
    id              SERIAL          PRIMARY KEY,
    name            VARCHAR(255)    NOT NULL,
    description     TEXT,
    price           NUMERIC(10, 2)  NOT NULL CHECK (price >= 0),
    stock_quantity  INT             NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    category        VARCHAR(100),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Required for Debezium to include full row in before-image
ALTER TABLE products REPLICA IDENTITY FULL;

-- Index for common query patterns
CREATE INDEX idx_products_category ON products (category);
CREATE INDEX idx_products_is_active ON products (is_active);

-- ─────────────────────────────────────────────────────────
-- CUSTOMERS TABLE
-- Simulates a customer registry.
-- Demonstrates CDC on a different table / topic.
--
-- NOTE: No real PII. Emails follow a synthetic pattern.
-- ─────────────────────────────────────────────────────────
CREATE TABLE customers (
    id              SERIAL          PRIMARY KEY,
    first_name      VARCHAR(100)    NOT NULL,
    last_name       VARCHAR(100)    NOT NULL,
    email           VARCHAR(255)    NOT NULL UNIQUE,
    phone           VARCHAR(20),
    country         VARCHAR(100),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

ALTER TABLE customers REPLICA IDENTITY FULL;

CREATE INDEX idx_customers_email ON customers (email);
CREATE INDEX idx_customers_country ON customers (country);

-- ─────────────────────────────────────────────────────────
-- ORDERS TABLE
-- Simulates order lifecycle: PENDING → CONFIRMED → SHIPPED → DELIVERED
-- Demonstrates UPDATE events with state machine transitions.
-- ─────────────────────────────────────────────────────────
CREATE TABLE orders (
    id              SERIAL          PRIMARY KEY,
    customer_id     INT             NOT NULL REFERENCES customers(id),
    status          VARCHAR(50)     NOT NULL DEFAULT 'PENDING'
                                    CHECK (status IN ('PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    total_amount    NUMERIC(12, 2)  NOT NULL CHECK (total_amount >= 0),
    currency        VARCHAR(3)      NOT NULL DEFAULT 'USD',
    shipping_address TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_status ON orders (status);

-- ─────────────────────────────────────────────────────────
-- ORDER ITEMS TABLE
-- Line items within an order.
-- Demonstrates a child table that references both orders and products.
-- ─────────────────────────────────────────────────────────
CREATE TABLE order_items (
    id              SERIAL          PRIMARY KEY,
    order_id        INT             NOT NULL REFERENCES orders(id),
    product_id      INT             NOT NULL REFERENCES products(id),
    quantity        INT             NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10, 2)  NOT NULL CHECK (unit_price >= 0),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

ALTER TABLE order_items REPLICA IDENTITY FULL;

CREATE INDEX idx_order_items_order_id ON order_items (order_id);
CREATE INDEX idx_order_items_product_id ON order_items (product_id);

-- ─────────────────────────────────────────────────────────
-- TRIGGER: auto-update updated_at on every UPDATE
--
-- WHY: Without this, updated_at stays at the insertion time
-- even after updates. Batch polling systems use updated_at
-- to detect changes. CDC doesn't need it (WAL captures
-- everything), but it's good practice and useful for debugging.
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ─────────────────────────────────────────────────────────
-- Verify setup (output appears in docker compose logs postgres)
-- ─────────────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE '=== SyncStream schema initialized ===';
    RAISE NOTICE 'Tables: products, customers, orders, order_items';
    RAISE NOTICE 'REPLICA IDENTITY FULL: set on all tables';
    RAISE NOTICE 'Triggers: updated_at auto-update on all mutable tables';
END $$;
