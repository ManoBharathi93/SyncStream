CREATE TABLE IF NOT EXISTS replay_requests (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    start_timestamp TEXT,
    start_offset TEXT,
    end_offset TEXT,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_replay_requests_topic
    ON replay_requests (topic);

CREATE INDEX IF NOT EXISTS ix_replay_requests_status
    ON replay_requests (status);

CREATE INDEX IF NOT EXISTS ix_replay_requests_created_at
    ON replay_requests (created_at);

CREATE TABLE IF NOT EXISTS management_audit (
    id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_management_audit_target
    ON management_audit (target_type, target_id);
