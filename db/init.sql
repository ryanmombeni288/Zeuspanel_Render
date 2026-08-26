-- ZEUS Panel reference schema for PostgreSQL.
--
-- NOTE: You normally do NOT need to run this file manually. The application's
-- DbService.ensureSchema() creates and migrates these tables automatically on
-- first request (identical to how it did on Cloudflare D1). This file exists
-- only as a human-readable reference / manual provisioning option.

CREATE TABLE IF NOT EXISTS users (
    id                      SERIAL PRIMARY KEY,
    username                TEXT UNIQUE,
    uuid                    TEXT,
    limit_gb                REAL,
    expiry_days             INTEGER,
    ips                     TEXT,
    connection_type         TEXT,
    tls                     TEXT,
    port                    INTEGER,
    used_gb                 REAL DEFAULT 0,
    is_active               INTEGER DEFAULT 1,
    last_active             INTEGER,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    advanced_frag           TEXT DEFAULT NULL,
    cipher_suites           TEXT DEFAULT NULL,
    tls_mask                TEXT DEFAULT NULL,
    fingerprint             TEXT DEFAULT 'chrome',
    max_connections         INTEGER,
    limit_req               INTEGER,
    used_req                INTEGER DEFAULT 0,
    ip_limit                INTEGER DEFAULT NULL,
    active_ips              TEXT DEFAULT NULL,
    block_porn              INTEGER DEFAULT 0,
    block_ads               INTEGER DEFAULT 0,
    frag_len                TEXT DEFAULT '200-3000',
    frag_int                TEXT DEFAULT '1-2',
    lifetime_used_gb        REAL DEFAULT 0,
    user_proxy_ip           TEXT DEFAULT NULL,
    user_proxy_iata         TEXT DEFAULT NULL,
    user_socks5             TEXT DEFAULT NULL,
    auto_reset_vol_days     INTEGER DEFAULT 0,
    auto_reset_req_days     INTEGER DEFAULT 0,
    last_reset_vol_time     INTEGER DEFAULT 0,
    last_reset_req_time     INTEGER DEFAULT 0,
    auto_rotate_ip          INTEGER DEFAULT 1,
    rotate_time             INTEGER DEFAULT 0,
    ip_operator             TEXT DEFAULT 'all',
    ip_count                INTEGER DEFAULT 15,
    last_rotate_time        INTEGER DEFAULT 0,
    auto_rotate_user_proxy  INTEGER DEFAULT 0,
    start_on_first_connect  INTEGER DEFAULT 0,
    first_connection_time   INTEGER DEFAULT NULL,
    trojan_hash             TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS users_uuid_idx       ON users (uuid);
CREATE INDEX IF NOT EXISTS users_trojan_hash_idx ON users (trojan_hash);
CREATE INDEX IF NOT EXISTS users_username_lower_idx ON users (lower(username));
