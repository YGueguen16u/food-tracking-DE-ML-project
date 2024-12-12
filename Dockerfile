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

# Copy the project files
COPY data_engineering/ ./data_engineering/
COPY setup.py .
COPY README.md .

# Create directories for data
RUN mkdir -p data/raw data/processed

# Set Python path
ENV PYTHONPATH=/app

# Command to run tests
CMD ["pytest", "data_engineering/test/"]
