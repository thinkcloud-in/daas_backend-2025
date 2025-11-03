# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.12.1-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libc-dev \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# # Copy the requirements file into the container at /app
# COPY requirements.txt .

# # Install pipenv
# RUN python -m pip install pipenv

# # Install dependencies into the virtual environment
# RUN python -m pipenv install --system --deploy --ignore-pipfile


RUN pip install --upgrade pip

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies using pip (from requirements.txt)
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the working directory
COPY . .

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]