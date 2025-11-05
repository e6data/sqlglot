FROM python:3.12-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc g++ cmake make libxml2-dev libxslt-dev openssl

# Copy all files needed for installation
COPY pyproject.toml setup.py ./
COPY sqlglotrs sqlglotrs
COPY sqlglot sqlglot
COPY apis apis

# Install dependencies only (without editable mode to avoid symlink issues)
RUN pip install --no-cache-dir .

# Final stage
FROM python:3.12-alpine

WORKDIR /app

# Install only runtime dependencies (no build tools)
RUN apk add --no-cache libxml2 libxslt openssl wget && \
    adduser --home /app e6 --disabled-password

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy only the entry point script
COPY converter_api.py .

USER e6
EXPOSE 8100

HEALTHCHECK none

CMD ["python", "converter_api.py"]
