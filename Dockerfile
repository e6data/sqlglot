FROM python:3.12-alpine

# Set the working directory in the container
WORKDIR /app

# Install dependencies required for building certain packages, including pyarrow
RUN apk add --no-cache gcc g++ cmake make libxml2-dev libxslt-dev \
    && apk add --no-cache py3-pyarrow openssl && \
    adduser --home /app e6 --disabled-password

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install specific FastAPI, Uvicorn, and multipart dependencies
RUN pip install fastapi==0.115.4 uvicorn==0.32.0 python-multipart 

# Copy the rest of the application code into the container
COPY . .

# Make port 8100 available to the world outside this container
USER e6

HEALTHCHECK none

# Run the FastAPI app using Uvicorn
CMD ["uvicorn", "converter_api:app", "--host", "0.0.0.0", "--port", "8100", "--workers", "5"]
