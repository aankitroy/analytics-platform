-- Insert a test client with 'pending' status for the provisioning DAG to process
-- This client will be picked up by the client_provisioning_dag

-- Generate a UUID for the test client
-- You can replace this with a specific UUID if needed
INSERT INTO clients (client_id, api_key_hash, ch_database_name, status)
VALUES (
    gen_random_uuid(), -- Generate a random UUID
    '003ea86ef7b73b39cb972fa0a5c601be44a749f56a5892d9b477ad2d05c87329', -- SHA-256 hash of test API key
    'client_pending_test', -- ClickHouse database name to be created
    'pending' -- Set status to 'pending' for the provisioning DAG to process
)
ON CONFLICT (client_id) DO NOTHING;

-- Print the inserted client ID for reference
SELECT client_id, ch_database_name, status 
FROM clients 
WHERE ch_database_name = 'client_pending_test'; 