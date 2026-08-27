FROM python:3.12-alpine

# Set the working directory in the container
WORKDIR /app

# Install dependencies required for building certain packages
RUN apk add --no-cache gcc g++ cmake make libxml2-dev libxslt-dev openssl && \
    adduser --home /app e6 --disabled-password

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install specific FastAPI, Uvicorn, and multipart dependencies
RUN pip install fastapi==0.115.4 uvicorn==0.32.0 python-multipart 

# Copy the rest of the application code into the container
COPY . .

# Build the Rust tokenizer (sqlglotrs) so sqlglot uses the fast Rust tokenizer (~5x faster
# tokenize). Toolchain is added and removed in the same layer to keep the final image lean.
RUN apk add --no-cache rust cargo patchelf && \
    pip install --no-cache-dir maturin && \
    (cd sqlglotrs && maturin build --release && pip install --no-cache-dir target/wheels/*.whl) && \
    apk del rust cargo patchelf && rm -rf sqlglotrs/target /root/.cargo /root/.rustup
# sqlglotrs is now available but OFF by default (converter_api defaults SQLGLOTRS_TOKENIZER=0 ->
# pure-Python tokenizer). Enable the Rust tokenizer per run with -e SQLGLOTRS_TOKENIZER=1.

# Make port 8100 available to the world outside this container
USER e6
EXPOSE 8100

HEALTHCHECK none

# Run the FastAPI app using Uvicorn
# Workers will be calculated dynamically based on CPU cores
CMD ["python", "converter_api.py"]
