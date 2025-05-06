# Analytics Platform - Airflow Setup

This repository contains the configuration for running Apache Airflow with external PostgreSQL and Redis services.

## Prerequisites

- Docker and Docker Compose installed
- Access to external PostgreSQL and Redis services
- Environment variables configured (see `.env` file)

## Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
# Airflow Image
AIRFLOW_IMAGE=apache/airflow:2.8.0
AIRFLOW_HOME=/opt/airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0

# External PostgreSQL Configuration
EXTERNAL_POSTGRES_HOST=*****
EXTERNAL_POSTGRES_PORT=5432
POSTGRES_USER=****
POSTGRES_PASSWORD=***
POSTGRES_DB=airflow

# External Redis Configuration
EXTERNAL_REDIS_HOST=****
EXTERNAL_REDIS_PORT=6379
EXTERNAL_REDIS_USER=***
REDIS_PASSWORD=*****
REDIS_DB_BROKER=0
REDIS_DB_RESULT=1

# Airflow Webserver Secret Key (generate a secure random key)
AIRFLOW_WEBSERVER_SECRET_KEY=your-secret-key-here
```

## Directory Structure

```
.
├── dags/                  # Airflow DAGs directory
├── logs/                  # Airflow logs directory
├── config/               # Airflow configuration files
├── plugins/              # Custom Airflow plugins
├── docker-compose.yaml   # Docker Compose configuration
└── .env                  # Environment variables
```

## Services

The setup includes the following services:

1. **airflow-init**: Initializes the Airflow database and creates the admin user
2. **airflow-webserver**: Airflow web interface (port 8080)
3. **airflow-scheduler**: Airflow scheduler service
4. **airflow-worker**: Celery worker for task execution

## Getting Started

1. Create the required directories:

   ```bash
   mkdir -p dags logs config plugins
   ```

2. Start the services:

   ```bash
   docker-compose up -d
   ```

3. Access the Airflow web interface:
   - URL: http://localhost:8080
   - Default credentials:
     - Username: admin
     - Password: admin (change this immediately after first login)

## Monitoring

- Airflow Web UI: http://localhost:8080
- Optional: Uncomment the Flower service in docker-compose.yaml for Celery monitoring
  - Flower UI: http://localhost:5555

## Security Notes

1. Change the default admin password immediately after first login
2. Keep your `.env` file secure and never commit it to version control
3. Consider using a secrets management solution for production deployments

## Troubleshooting

1. If services fail to start, check the logs:

   ```bash
   docker-compose logs -f
   ```

2. For database connection issues:

   - Verify PostgreSQL credentials
   - Check if the database exists
   - Ensure network connectivity

3. For Redis connection issues:
   - Verify Redis credentials
   - Check if Redis is accessible
   - Ensure network connectivity

## License

This project is licensed under the Apache License 2.0.
