FROM python:3.12-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc g++ cmake make libxml2-dev libxslt-dev openssl

# Copy necessary files for package installation
COPY pyproject.toml setup.py ./
COPY sqlglotrs/Cargo.toml sqlglotrs/Cargo.toml

# Install the package dependencies
RUN pip install --no-cache-dir -e .

# Copy application code
COPY . .

# Final stage
FROM python:3.12-alpine

WORKDIR /app

# Install only runtime dependencies (no build tools)
RUN apk add --no-cache libxml2 libxslt openssl && \
    adduser --home /app e6 --disabled-password

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --from=builder /app /app

USER e6
EXPOSE 8100

HEALTHCHECK none

CMD ["python", "converter_api.py"]
