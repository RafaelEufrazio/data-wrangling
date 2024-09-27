FROM python:3.11-alpine

# Set the working directory
WORKDIR /code

# Copy the requirements file
COPY ./requirements.txt requirements.txt

# Install dependencies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the FastAPI application code
COPY ./app/ app/

# Command to run the FastAPI app
CMD uvicorn app.main:app --port 8000 --host 0.0.0.0 --reload