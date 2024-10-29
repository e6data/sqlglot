FROM python:3.12-alpine

# Set the working directory in the container
WORKDIR /app

# Install dependencies required for building certain packages, including pyarrow
RUN apk add --no-cache gcc g++ cmake make libxml2-dev libxslt-dev \
    && apk add --no-cache py3-pyarrow

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install specific FastAPI, Uvicorn, and multipart dependencies
RUN pip install fastapi==0.103.1 uvicorn==0.23.2 python-multipart

# Copy the rest of the application code into the container
COPY . .

# Make port 8100 available to the world outside this container
EXPOSE 8100

# Run the FastAPI app using Uvicorn
CMD ["uvicorn", "converter_api:app", "--host", "0.0.0.0", "--port", "8100", "--workers", "5"]
