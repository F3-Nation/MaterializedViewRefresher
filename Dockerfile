# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy code
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables if desired (can also set in Cloud Run)
ENV PYTHONUNBUFFERED=1

# Run the job
CMD ["python", "main.py"]
