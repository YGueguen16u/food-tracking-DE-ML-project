# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for data with proper permissions
RUN mkdir -p data/raw data/processed && \
    chmod -R 755 /app

# Copy the project files with proper permissions
COPY . .
RUN chmod -R 755 /app

# Set Python path
ENV PYTHONPATH=/app

# Command to run tests
# CMD ["pytest", "data_engineering/test/"]

CMD ["python", "-m", "data_engineering"]
