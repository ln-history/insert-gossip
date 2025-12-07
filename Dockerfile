FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (gcc needed for some python builds)
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# We don't set an entrypoint/cmd here because this is a "Run Once" tool.
# We will trigger it manually via docker compose run.
CMD ["python", "-u", "main.py"]