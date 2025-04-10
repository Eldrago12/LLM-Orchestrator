FROM python:3.9-slim
WORKDIR /app

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Expose port 5000 for the Flask app
EXPOSE 8080

CMD ["python", "app.py"]
