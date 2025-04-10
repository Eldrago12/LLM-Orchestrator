FROM python:3.9-slim

WORKDIR /app

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Command is specified in docker-compose.yml
# CMD ["python", "data_cleaner_consumer.py"]
