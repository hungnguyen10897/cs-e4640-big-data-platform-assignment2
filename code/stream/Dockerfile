FROM python:3.8.2

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/app"

# CMD ["python", "producer.py"]
