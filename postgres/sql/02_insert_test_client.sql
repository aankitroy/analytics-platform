-- Insert a test client for development and testing
-- **IMPORTANT: Replace the placeholder values with your actual test values**

-- Test client details
-- Example content of 02_insert_test_client.sql
-- **REMEMBER TO REPLACE PLACEHOLDERS WITH ACTUAL VALUES**
INSERT INTO clients (client_id, api_key_hash, ch_database_name, status)
VALUES (
    'a1b2c3d4-e5f6-7890-1234-567890abcdef', -- Use a consistent test client UUID
    'c44b61ce039b4637f3a7cb92dcc445876a4720d25d1c9614fe08fc734f2da20f',     -- SHA-256 hash of test API key
    'client_test', -- Match this with the CH DB name you'll create
    'active'                                -- Set status to 'active' for the router to pick it up
)
ON CONFLICT (client_id) DO NOTHING;