# Use Apache Airflow as the base image
FROM apache/airflow:2.7.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

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
