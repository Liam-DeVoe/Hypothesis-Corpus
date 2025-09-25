FROM python:3.13-slim

# Install git and other system dependencies
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy analysis scripts
COPY analyzer/ /app/analyzer/

# Set up non-root user for security
RUN useradd -m -u 1000 testrunner && \
    chown -R testrunner:testrunner /app

USER testrunner

# Default command
CMD ["python", "-m", "analyzer.test_runner"]
