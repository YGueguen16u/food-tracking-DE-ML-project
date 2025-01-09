# Use Python as base image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for data with proper permissions
RUN mkdir -p data/raw data/processed && \
    chmod -R 755 /app

# Copy the project files
COPY . .

# Set Python path
ENV PYTHONPATH=/app

# Copy .env file if it exists
COPY .env* ./aws_s3/ 2>/dev/null || :

# Expose Streamlit port
EXPOSE 8501

# Command to run Streamlit
CMD ["streamlit", "run", "streamlit_app/Home.py"]
