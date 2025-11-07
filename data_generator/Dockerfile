FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set work directory
WORKDIR /app

# Copy pyproject.toml first for better caching
COPY pyproject.toml .

# Install Python dependencies using uv (without editable install)
RUN uv pip install --system pandas azure-storage-blob azure-identity deltalake faker python-dotenv pyarrow

# Copy the application code
COPY . .

# Default command
CMD ["python", "generate_data.py"]
