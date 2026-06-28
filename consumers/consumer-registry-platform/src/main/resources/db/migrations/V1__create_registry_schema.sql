CREATE TABLE IF NOT EXISTS consumer_registrations (
    id TEXT PRIMARY KEY,
    consumer TEXT NOT NULL,
    topic TEXT NOT NULL,
    environment TEXT NOT NULL,
    owner_team TEXT NOT NULL,
    status TEXT NOT NULL,
    config_json TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_consumer_topic_env
    ON consumer_registrations (consumer, topic, environment);

CREATE INDEX IF NOT EXISTS ix_registrations_topic
    ON consumer_registrations (topic);

CREATE INDEX IF NOT EXISTS ix_registrations_owner_team
    ON consumer_registrations (owner_team);

CREATE INDEX IF NOT EXISTS ix_registrations_status
    ON consumer_registrations (status);

CREATE INDEX IF NOT EXISTS ix_registrations_environment
    ON consumer_registrations (environment);

CREATE TABLE IF NOT EXISTS registration_audit (
    id TEXT PRIMARY KEY,
    registration_id TEXT NOT NULL,
    action TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (registration_id) REFERENCES consumer_registrations (id)
);

CREATE INDEX IF NOT EXISTS ix_audit_registration_id
    ON registration_audit (registration_id);

CREATE INDEX IF NOT EXISTS ix_audit_created_at
    ON registration_audit (created_at);

CREATE TABLE IF NOT EXISTS registry_events (
    id TEXT PRIMARY KEY,
    registration_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_registry_events_registration_id
    ON registry_events (registration_id);

CREATE TABLE IF NOT EXISTS routing_state (
    route_key TEXT PRIMARY KEY,
    registration_id TEXT NOT NULL,
    consumer TEXT NOT NULL,
    topic TEXT NOT NULL,
    environment TEXT NOT NULL,
    active INTEGER NOT NULL,
    effective_config_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
