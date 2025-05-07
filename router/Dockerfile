# your-analytics-monorepo/router/Dockerfile
# Use a lightweight Python base image
# Using a specific version is better than 'latest' for reproducibility
FROM python:3.10-slim-buster

# Set the working directory inside the container
# All subsequent instructions will run relative to this directory
WORKDIR /app

# Copy the requirements file into the working directory
# This step is key for installing dependencies
COPY requirements.txt .

# Install Python dependencies
# --no-cache-dir helps keep the image size down
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port the router will listen on (if it has an HTTP endpoint like health check)
# ROUTER_LISTEN_PORT is defined as a build argument and then set as an environment variable
ARG ROUTER_LISTEN_PORT=8080
EXPOSE ${ROUTER_LISTEN_PORT}

# Set environment variables with default values.
# These can be overridden by the environment variables passed from docker-compose.yml.
ENV KAFKA_TOPIC_RAW="shared_raw_events" \
    KAFKA_TOPIC_DLQ="shared_dead_letter" \
    KAFKA_CONSUMER_GROUP_ID="analytics_router_group" \
    CLIENT_CACHE_TTL_SECONDS="600" \
    ROUTER_LISTEN_PORT=${ROUTER_LISTEN_PORT}
# Note: Database connection details (HOST, PORT, USER, PASSWORD) are NOT set here
# as they contain secrets and come from the environment when running the container.

# Command to run the application when the container starts
# This tells Docker to execute your main Python script
CMD ["python", "/app/src/main.py"]