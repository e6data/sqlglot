# Use the official lightweight Python image
FROM python:3.9

# Set the working directory in the container
RUN adduser --home /app e6 --disabled-password
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install fastapi==0.103.1
RUN pip install uvicorn==0.23.2
RUN pip install python-multipart

# Copy the rest of the application code into the container
COPY . .

# Make port 8000 available to the world outside this container
EXPOSE 8100

USER e6

HEALTHCHECK none

# Run the FastAPI app using Uvicorn
CMD ["uvicorn", "converter_api:app", "--host", "0.0.0.0", "--port", "8100", "--workers", "5"]
