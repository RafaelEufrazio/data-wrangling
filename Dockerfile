FROM python:3.11-alpine

# Set the working directory
WORKDIR /code

# Copy the requirements file
COPY ./requirements.txt requirements.txt

# Install dependencies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy files into container
COPY ./app ./app

# Expose port 8000
EXPOSE 8000

# Run command on start
ENTRYPOINT ["uvicorn", "app.main:app", "--port", "8000", "--host", "0.0.0.0"]