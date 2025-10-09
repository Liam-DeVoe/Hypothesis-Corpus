FROM python:3.13

RUN apt-get update && apt-get install -y \
    git \
    python3-dev \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code
WORKDIR /app
CMD ["python", "-m", "analysis.test_runner"]
