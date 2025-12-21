--------------------------------------------------
-- USERS TABLE
--------------------------------------------------
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    user_id        VARCHAR(36) PRIMARY KEY,
    email          VARCHAR(255) NOT NULL UNIQUE,
    full_name      TEXT,
    role           VARCHAR(50) NOT NULL DEFAULT 'user',
    status         VARCHAR(32) NOT NULL DEFAULT 'Active',
    password_hash  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_users_email ON users(email);
