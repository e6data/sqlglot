FROM python:3.12-alpine

# Set the working directory in the container
WORKDIR /app

# Install dependencies required for building certain packages including Rust
RUN apk add --no-cache gcc g++ cmake make libxml2-dev libxslt-dev openssl curl && \
    adduser --home /app e6 --disabled-password

# Install Rust toolchain for building sqlglotrs
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install specific FastAPI, Uvicorn, multipart dependencies, maturin
RUN pip install fastapi==0.115.4 uvicorn==0.32.0 python-multipart maturin[patchelf]

# Copy the rest of the application code into the container
COPY . .

# Build and install the Rust tokenizer (sqlglotrs)
RUN cd sqlglotrs && \
    maturin build --release && \
    pip install target/wheels/*.whl

# Enable Rust tokenizer by default
ENV ENABLE_RUST_TOKENIZER=true
ENV SQLGLOTRS_TOKENIZER=1

# Make port 8100 available to the world outside this container
USER e6
EXPOSE 8100

HEALTHCHECK none

# Run the FastAPI app using Uvicorn
# Workers will be calculated dynamically based on CPU cores
CMD ["python", "converter_api.py"]