# Multi-stage build for health-stack gateway

# Stage 1: Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install uv
RUN pip install uv

# Copy dependency files
COPY pyproject.toml .python-version ./

# Install dependencies
RUN uv sync --no-dev

# Stage 2: Development stage
FROM python:3.11-slim as development

WORKDIR /app

# Install uv and openssl
RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*
RUN pip install uv

# Copy dependency files
COPY pyproject.toml .python-version ./

# Install all dependencies including dev dependencies
RUN uv sync

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"

# Expose port
EXPOSE 3000

# Run with hot reload
CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "3000"]

# Stage 3: Production runtime stage
FROM python:3.11-slim as production

WORKDIR /app

# Install uv and openssl
RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*
RUN pip install uv

# Copy dependencies from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import httpx; httpx.get('http://localhost:3000/health')"

# Run application
CMD ["python", "main.py"]
