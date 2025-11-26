# Multi-stage Dockerfile for Flink Iceberg Consumer
# Base image: Apache Flink 1.18 with Python 3.11

FROM flink:1.18-python3.11 AS base

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast Python package management
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /opt/flink-app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install Python dependencies using uv
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .

# Set Python path to include application directory
ENV PYTHONPATH=/opt/flink-app:$PYTHONPATH

# Copy Flink configuration files
COPY config/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
COPY config/log4j2.properties /opt/flink/conf/log4j2.properties

# Create directories for checkpoints and state
RUN mkdir -p /opt/flink/checkpoints /opt/flink/state

# Set proper permissions
RUN chown -R flink:flink /opt/flink-app /opt/flink/checkpoints /opt/flink/state

# Switch back to flink user
USER flink

# Expose Flink ports
# 6123: JobManager RPC
# 6124: TaskManager RPC
# 8081: JobManager Web UI
# 9249: Prometheus metrics
EXPOSE 6123 6124 8081 9249

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8081/overview || exit 1

# Default entrypoint (can be overridden)
ENTRYPOINT ["/docker-entrypoint.sh"]

# Default command: run as standalone job
CMD ["standalone-job", "--python", "/opt/flink-app/main.py"]


# Development stage with additional tools
FROM base AS development

USER root

# Install development dependencies
RUN uv sync --frozen

# Install additional dev tools
RUN pip install --no-cache-dir \
    ipython \
    jupyter

USER flink

CMD ["bash"]


# Production stage (optimized)
FROM base AS production

# Remove unnecessary files
USER root
RUN find /opt/flink-app -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
RUN find /opt/flink-app -type f -name "*.pyc" -delete 2>/dev/null || true
RUN find /opt/flink-app -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
RUN find /opt/flink-app -type d -name "examples" -exec rm -rf {} + 2>/dev/null || true

USER flink

# Production command
CMD ["standalone-job", "--python", "/opt/flink-app/main.py"]
