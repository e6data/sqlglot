FROM python:3.12-alpine AS builder

WORKDIR /app

# Toolchain lives only in this stage; the runtime image gets just the installed packages.
RUN apk add --no-cache gcc g++ musl-dev rust cargo patchelf

# Runtime dependencies of converter_api.py (requirements.txt is the dev/frontend set)
RUN pip install --no-cache-dir --prefix=/install \
    fastapi==0.115.4 uvicorn==0.32.0 python-multipart==0.0.20 thrift==0.21.0 python-dateutil==2.9.0.post0

# Build the Rust tokenizer (sqlglotrs) so sqlglot uses the fast Rust tokenizer (~5x faster
# tokenize).
COPY sqlglotrs sqlglotrs
RUN pip install --no-cache-dir maturin && \
    (cd sqlglotrs && maturin build --release && pip install --no-cache-dir --prefix=/install target/wheels/*.whl)
# sqlglotrs is now available but OFF by default (converter_api defaults SQLGLOTRS_TOKENIZER=0 ->
# pure-Python tokenizer). Enable the Rust tokenizer per run with -e SQLGLOTRS_TOKENIZER=1.

FROM python:3.12-alpine

# Set the working directory in the container
WORKDIR /app

RUN apk add --no-cache libstdc++ && \
    adduser --home /app e6 --disabled-password

COPY --from=builder /install /usr/local

# Copy only the application code needed at runtime
COPY converter_api.py log_collector.py formatting_utils.py ./
COPY sqlglot sqlglot
COPY apis apis
COPY guardrail guardrail

# Make port 8100 available to the world outside this container
USER e6
EXPOSE 8100

HEALTHCHECK none

# Run the FastAPI app using Uvicorn
# Workers will be calculated dynamically based on CPU cores
CMD ["python", "converter_api.py"]
