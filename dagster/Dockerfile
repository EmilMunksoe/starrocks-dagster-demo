FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set work directory
WORKDIR /opt/dagster/app

# Copy pyproject.toml first for better caching
COPY pyproject.toml .

# Install Python dependencies using uv from pyproject.toml
RUN uv pip install --system .

# Copy the application code
COPY . .

# Expose port
EXPOSE 3000

# Default command (will be overridden in compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]