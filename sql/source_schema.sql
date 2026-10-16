-- OLTP source ("shop" service database)
CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    email       TEXT NOT NULL,          -- PII: not replicated (see config/tables.yml)
    country     TEXT NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'standard',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id    BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers (customer_id),
    status      TEXT NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    currency    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full before-images so deletes/updates carry every column (needed for soft deletes
-- and auditing). Costs extra WAL volume; acceptable for these low-width tables.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

-- Heartbeat table: keeps the replication slot advancing when the captured tables are
-- quiet, preventing WAL from piling up on the source (a classic Debezium outage).
CREATE TABLE debezium_heartbeat (id INT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL);

-- Replication user. Filtered publication autocreate needs table ownership, so this demo
-- grants it; in production an admin pre-creates the publication and the user keeps SELECT only.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT SELECT ON customers, orders TO debezium;
GRANT INSERT, UPDATE ON debezium_heartbeat TO debezium;
GRANT CREATE ON DATABASE shop TO debezium;  -- for publication autocreate (filtered)
ALTER TABLE customers OWNER TO debezium;
ALTER TABLE orders OWNER TO debezium;
