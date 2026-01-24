# 1. Use an official, lightweight Python base image
FROM python:3.11-slim

# 2. Set environment variables to ensure Python output is sent straight to logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Set the working directory inside the container
WORKDIR /app

# 4. Copy only the requirements file first to take advantage of Docker cache
COPY requirements.txt .

# 5. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy the rest of your application code into the container
COPY . .

# Note: We don't define a 'CMD' here because the docker-compose.yml
# handles the different start commands for the API and the Worker.