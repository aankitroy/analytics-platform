-- Create metadata tables for client management and schema version tracking
-- These tables are used by the Router to validate clients and track schema migrations

-- Table for storing client information
CREATE TABLE IF NOT EXISTS clients (
    client_id UUID PRIMARY KEY,                    -- Unique identifier for each client
    api_key_hash TEXT NOT NULL,                    -- Hashed API key for authentication
    ch_database_name TEXT NOT NULL UNIQUE,         -- ClickHouse database name for this client
    status TEXT CHECK(status IN ('active', 'inactive', 'pending')) DEFAULT 'pending',  -- Client status
    created_at TIMESTAMP DEFAULT NOW(),            -- When the client was created
    updated_at TIMESTAMP DEFAULT NOW()             -- When the client was last updated
);

-- Table for tracking schema migrations per client
CREATE TABLE IF NOT EXISTS schema_versions (
    client_id UUID REFERENCES clients(client_id) ON DELETE CASCADE,  -- Reference to client
    migration_id VARCHAR(255) NOT NULL,                              -- Migration identifier
    applied_at TIMESTAMP DEFAULT NOW(),                              -- When migration was applied
    PRIMARY KEY (client_id, migration_id)                            -- Composite primary key
);

-- Create an index on api_key_hash for faster lookups
CREATE INDEX IF NOT EXISTS idx_clients_api_key_hash ON clients(api_key_hash);

-- Create an index on status for filtering active clients
CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);

