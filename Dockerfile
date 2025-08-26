# Use a lightweight Python image
FROM python:3.13-alpine

# Install system deps
RUN apk update && apk add --no-cache \
    curl \
    gcc \
    libressl-dev \
    musl-dev \
    py3-pip \
    python3-dev \
    libevent-dev

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Run the script
CMD ["python", "main.py"]