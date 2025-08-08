# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Create a non-root user to run the application
RUN useradd --create-home appuser
USER appuser

# Add the user's local bin directory to the PATH
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Copy the requirements file and install dependencies
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code
COPY --chown=appuser:appuser . .

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variables for Flask
ENV FLASK_APP=run.py

# Run the command to start the Gunicorn server
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "run:app"]
