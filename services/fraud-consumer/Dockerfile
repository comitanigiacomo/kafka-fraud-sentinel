FROM python:3.11-slim
WORKDIR /app/services/fraud-consumer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "consumer.py"]
