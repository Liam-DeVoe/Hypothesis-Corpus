FROM python:3.13

# Install git
RUN apt-get update && apt-get install -y \
    git \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy analysis scripts
COPY analyzer/ /app/analyzer/

# Default command
CMD ["python", "-m", "analyzer.test_runner"]
