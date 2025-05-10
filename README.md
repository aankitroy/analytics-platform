# Product Analytics Data Pipeline

## Introduction

This repository contains the technical implementation for a robust, multi-tenant product analytics data pipeline. The pipeline is designed to handle scalable, secure ingestion and storage of analytics events, providing both real-time and batch processing capabilities with per-client data isolation and flexible schemas.

## Goals

- Enable scalable, secure ingestion, and storage of analytics events.
- Provide reliable, real-time, and batch data ingestion pipelines.
- Maintain data isolation and schema flexibility per client.
- Implement effective monitoring, alerting, and operational simplicity.
- Ensure traceability and auditability for every data event, from ingestion to storage.
- Prioritize visibility and automation for solo developer management.

## Architecture Overview

The pipeline consists of several loosely coupled components:

- **Event Producers:** Applications sending analytics events.
- **Apache Kafka:** Acts as a scalable buffer and transport layer for raw events (`shared_raw_events`) and dead-letter messages (`shared_dead_letter`).
- **Custom Ingestion Router:** Consumes raw events from Kafka, validates clients, and routes data to client-specific ClickHouse databases. Uses Redis for caching client metadata and PostgreSQL for the source of truth for client configuration.
- **Redis:** Used as a high-speed cache for client metadata by the Router.
- **Apache Airflow:** The workflow orchestration engine responsible for:
  - Client Provisioning and Deprovisioning
  - Schema Migration Automation
  - Batch Data Ingestion (using dlt)
  - DLQ Reprocessing
- **PostgreSQL:** Serves as the centralized metadata store for client configurations (`clients`, `schema_versions` tables) and also as Airflow's metadata database. **(Assumed to be an existing, external deployment)**
- **ClickHouse:** The columnar analytics database for storing event data, with a dedicated database instance per client for strong isolation. **(Assumed to be an existing, external deployment)**
- **Observability Stack:** (Planned for later phases) Prometheus, Grafana, Loki, Alertmanager for monitoring, logging, and alerting.

All components are designed to be independently scalable.

## Components & Technologies

- **Apache Kafka** (3.x)
- **PostgreSQL** (15.x) - External Metadata DB
- **ClickHouse** (24.x) - External Analytics DB
- **Apache Airflow** (2.8+)
- **Python/Go** - Custom Ingestion Router (Python implemented initially)
- **dlt** (0.4.x) - Batch ingestion framework
- **Redis**
- **Docker & Docker Compose** - Local Development/Single VM Deployment
- Kubernetes + Helm - (Future Production Deployment)
- Prometheus, Grafana, Loki, Alertmanager - (Future Observability)

## Prerequisites

Before you begin, ensure you have the following installed and configured on your local machine:

- **Git:** For cloning the repository.
- **Docker Desktop:** Includes Docker Engine and Docker Compose. (Ensure it's running).
- **Python 3.8+:** For running test scripts and local development if not using Docker for everything.
- **`psql` client:** Command-line client for PostgreSQL, for manual database setup and verification.
- **`clickhouse-client` client:** Command-line client for ClickHouse, for manual database setup and verification.
- **Access to an existing PostgreSQL instance:** This instance will be used for both client metadata and Airflow metadata. You need hostname/IP, port, database name, username, and password.
- **Access to an existing ClickHouse instance:** This instance will be used for storing client analytics data. You need hostname/IP, native port (usually 9000), HTTP port (usually 8123 or 8443), username, and password.

## Setup - Local Development Environment

This section guides you through setting up the local development environment using Docker Compose, connecting to your existing external databases.

1.  **Clone the Repository:**

    ```bash
    git clone <repository_url>
    cd your-analytics-monorepo
    ```

2.  **Prepare External Databases:**

    - **PostgreSQL Metadata DB:** Connect to your external PostgreSQL instance using `psql` and apply the schema for client metadata.
      ```bash
      # From your-analytics-monorepo/
      psql -h your_pg_host -p your_pg_port -U your_pg_user -d your_pg_db_name -f postgres/sql/01_create_metadata_tables.sql
      psql -h your_pg_host -p your_pg_port -U your_pg_user -d your_pg_db_name -f postgres/sql/02_insert_test_client.sql
      ```
      **Ensure you update `02_insert_test_client.sql` with a test `client_id`, the hash of a test API key, and a `ch_database_name` for your test client.**
    - **ClickHouse Analytics DB:** Connect to your external ClickHouse instance using `clickhouse-client`. Create a dedicated database for your test client and apply the baseline schema (with `event_properties` as `String`).
      ```bash
      # From your-analytics-monorepo/
      clickhouse-client --host your_ch_host --port your_ch_native_port --user your_ch_user --password your_ch_password --query "CREATE DATABASE IF NOT EXISTS client_your_test_client_uuid;"
      clickhouse-client --host your_ch_host --port your_ch_native_port --user your_ch_user --password your_ch_password --database client_your_test_client_uuid --query "$(cat clickhouse/schemas/01_events_base.sql)"
      # Verify schema: DESCRIBE TABLE client_your_test_client_uuid.events_base; (event_properties should be String)
      ```
      **Replace `client_your_test_client_uuid` with the `ch_database_name` you used in `02_insert_test_client.sql`.** Ensure `01_events_base.sql` defines `event_properties String`.

3.  **Create Local Environment File:**

    - Create a `.env` file in the `deploy/local/` directory. **This file contains secrets and should NOT be committed to Git.**
    - Populate it with the connection details for your external databases and other configuration needed by the Docker containers.

    ```bash
    # From your-analytics-monorepo/
    touch deploy/local/.env
    # Edit deploy/local/.env and add:
    ```

    ```dotenv
    # deploy/local/.env - **DO NOT COMMIT TO GIT!**

    # External PostgreSQL Metadata DB
    POSTGRES_METADATA_HOST=your_external_pg_host
    POSTGRES_METADATA_PORT=your_external_pg_port
    POSTGRES_METADATA_DB=your_external_pg_db_name
    POSTGRES_METADATA_USER=your_pg_user
    POSTGRES_METADATA_PASSWORD=your_pg_password

    # External ClickHouse Analytics DB
    CLICKHOUSE_HOST=your_external_ch_host
    CLICKHOUSE_PORT_NATIVE=your_external_ch_native_port # e.g., 9000
    CLICKHOUSE_PORT_HTTP=your_external_ch_http_port # e.g., 8123 or 8443
    CLICKHOUSE_USER=your_ch_user
    CLICKHOUSE_PASSWORD=your_ch_password

    # Airflow Specific (used by airflow-init user creation and webserver secret)
    AIRFLOW_WEBSERVER_SECRET_KEY=a_very_long_and_random_secret_key_for_local # CHANGE THIS
    AIRFLOW_CORE_DAGS_ARE_PAUSED_AT_CREATION=true
    AIRFLOW_CORE_LOAD_EXAMPLES=false
    AIRFLOW_API_ENABLE_XCOM_PICKLING=true # Use with caution
    ```

    **Replace placeholder values with your actual credentials and hostnames/IPs.**

4.  **Ensure Airflow Dependencies:**

    - Verify `airflow/requirements.txt` exists and contains `dlt[clickhouse]`, `psycopg2-binary`, `clickhouse-connect`, etc.
    - Verify `airflow/dags/clickhouse/schemas/01_events_base.sql` exists (copied in Step 2).

5.  **Run Airflow Database Migrations and Create User:**

    - This step initializes the Airflow metadata schema and creates the default admin user (`airflow`/`airflow`) in your external PostgreSQL database.

    ```bash
    # From your-analytics-monorepo/ (monorepo root)
    docker compose -f deploy/local/docker-compose.yml --env-file deploy/local/.env run --rm airflow-init_local

    # Check the output for success messages (migrations, user creation).
    # If it fails, check logs: docker compose -f deploy/local/docker-compose.yml logs airflow-init_local
    ```

6.  **Start the Docker Compose Stack:**

    - This brings up Kafka, Redis, Router, and all Airflow services.

    ```bash
    # From your-analytics-monorepo/ (monorepo root)
    docker compose -f deploy/local/docker-compose.yml --env-file deploy/local/.env up --build -d --remove-orphans

    # Check container status:
    docker compose -f deploy/local/docker-compose.yml ps
    # All services (analytics-router, kafka, redis, zookeeper, airflow-webserver, airflow-scheduler, airflow-worker) should be Up.
    ```

    - If you encounter port conflicts (e.g., 8080), modify the host port in `deploy/local/docker-compose.yml` for the conflicting service (likely router healthcheck or webserver).

7.  **Access Airflow UI and Configure Connections:**

    - Open your browser to `http://localhost:8080` (or the adjusted port). Log in with `airflow`/`airflow`.
    - Go to **Admin -> Connections**. Create/verify the following connections, testing each one:
      - `postgres_metadata_conn` (Postgres, using your external PG details)
      - `clickhouse_external_conn` (ClickHouse, using your external CH details - match port/protocol for dlt/clickhouse-connect)
      - `redis_local_conn` (Redis, Host: `redis`, Port: `6379`)
      - `kafka_local_conn` (Kafka, Brokers: `kafka:9093`)

8.  **Verify DAGs in Airflow UI:**
    - Go to the **DAGs** list. You should see `batch_ingestion_dag`, `client_provisioning_dag`, `client_deprovision_dag`, `schema_migration_dag`, `dlq_reprocessing_dag`.
    - They should be paused. Check for any "Invalid DAG file" errors.

## Usage - Local Development

1.  **Send Real-time Events:** Use a Kafka producer script (e.g., `send_test_event.py` as discussed in Phase 1) to send events to `localhost:9092`. Ensure the payload includes `client_id` and `api_key_hash` matching an active client in your external PG. Check Router logs (`docker compose logs analytics-router`) and ClickHouse (`client_your_test_client_uuid.events_base` table) to verify ingestion.

2.  **Run Batch Ingestion (using dlt):**

    - Ensure you have active clients in your external PG (`status = 'active'`).
    - In Airflow UI, find `batch_ingestion_dag`.
    - Unpause the DAG.
    - Trigger the DAG manually.
    - Monitor task logs in the UI for `run_ingestion_for_client`.
    - Verify data appears in the `users` and `page_views` tables in the client's ClickHouse database.

3.  **Run Client Provisioning:**

    - Connect to your external PG and insert a client with `status = 'pending'`.
    - In Airflow UI, find `client_provisioning_dag`.
    - Unpause the DAG.
    - Trigger the DAG manually.
    - Monitor task logs.
    - Verify client status becomes `active` in PG and the database/table is created in CH.

4.  **Run Client Deprovisioning:**

    - In Airflow UI, find `client_deprovision_dag`.
    - Unpause the DAG.
    - Trigger the DAG manually, providing the `client_id` to deprovision in the trigger configuration JSON.
    - Monitor task logs.
    - Verify client status becomes `deprovisioned` in PG and the database is dropped in CH. **Be careful with this!**

5.  **Run Schema Migration:**

    - Ensure you have active clients in external PG.
    - Add a new migration SQL file (e.g., `v2_...sql`) in `clickhouse/migrations/`.
    - If needed, manually update the `schema_versions` table in PG for a client to simulate which migrations they _have_ applied (so the new one is pending).
    - In Airflow UI, find `schema_migration_dag`.
    - Unpause the DAG.
    - Trigger the DAG manually.
    - Monitor task logs.
    - Verify the new schema change is applied to the client's CH database and recorded in the PG `schema_versions` table.

6.  **Run DLQ Reprocessing:**
    - Send an invalid event to Kafka (`localhost:9092`) that will end up in the `shared_dead_letter` topic (e.g., missing client ID, invalid API key hash).
    - In Airflow UI, find `dlq_reprocessing_dag`.
    - Unpause the DAG.
    - Trigger the DAG manually (or wait for its schedule).
    - Monitor task logs. Verify it consumes from the DLQ and attempts reprocessing.

## Deployment - Production VM

The `deploy/prod/` directory contains configurations and scripts intended for deployment on a single production VM (e.g., using a production `.env` file for secrets). This is outside the scope of the local setup detailed here but serves as the next step towards a production environment. Security considerations (TLS, proper secret management, restricted network access) are critical in this phase.

## Contributing

(Standard Contributing section - can be added later if needed)

## License

(Standard License section - e.g., MIT, Apache 2.0)
