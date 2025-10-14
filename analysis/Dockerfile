FROM python:3.12

RUN apt-get update && apt-get install -y \
    git \
    python3-dev \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN npm install -g @anthropic-ai/claude-code
WORKDIR /app
CMD ["python", "-m", "analysis.test_runner"]
