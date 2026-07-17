FROM python:3.10-slim

WORKDIR /app

# Install Java (required by Spark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Copy pyproject.toml first for dependency caching
COPY pyproject.toml .

# Install Python dependencies
RUN pip install --no-cache-dir -e ".[dev]"

# Copy source code
COPY src/ src/
COPY scripts/ scripts/

# Create data directory
RUN mkdir -p /app/data

# Expose Spark UI port (optional)
EXPOSE 4040

# Default command: run streaming job
CMD ["python", "-m", "pipeline.streaming_job"]
