#!/bin/bash

# Script to insert a test client with 'pending' status into the external PostgreSQL database
# This client will be picked up by the client_provisioning_dag

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    source .env
fi

# Check if required environment variables are set
if [ -z "$POSTGRES_METADATA_HOST" ] || [ -z "$POSTGRES_METADATA_PORT" ] || [ -z "$POSTGRES_METADATA_DB" ] || [ -z "$POSTGRES_METADATA_USER" ] || [ -z "$POSTGRES_METADATA_PASSWORD" ]; then
    echo "Error: Required environment variables are not set."
    echo "Please set the following environment variables:"
    echo "  POSTGRES_METADATA_HOST"
    echo "  POSTGRES_METADATA_PORT"
    echo "  POSTGRES_METADATA_DB"
    echo "  POSTGRES_METADATA_USER"
    echo "  POSTGRES_METADATA_PASSWORD"
    exit 1
fi

# Execute the SQL file
echo "Inserting test client with 'pending' status..."
PGPASSWORD=$POSTGRES_METADATA_PASSWORD psql -h $POSTGRES_METADATA_HOST -p $POSTGRES_METADATA_PORT -d $POSTGRES_METADATA_DB -U $POSTGRES_METADATA_USER -f postgres/sql/03_insert_pending_client.sql

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "Test client inserted successfully."
    echo "You can now run the client_provisioning_dag to process this client."
else
    echo "Error: Failed to insert test client."
    exit 1
fi 