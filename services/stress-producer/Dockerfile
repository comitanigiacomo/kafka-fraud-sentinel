FROM python:3.11-slim
WORKDIR /app/services/stress-producer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "stress_producer.py"]
