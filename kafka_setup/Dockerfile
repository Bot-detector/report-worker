FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY /src .

# Set the KAFKA_BROKER environment variable during container runtime
ENV KAFKA_BROKER=localhost:9094

CMD ["python", "main.py"]
