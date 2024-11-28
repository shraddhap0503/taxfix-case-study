# Use an existing Spark base image
FROM bitnami/spark:latest

# Set the working directory inside the container
WORKDIR /main

# Copy application files
COPY main /main

# Install additional Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt