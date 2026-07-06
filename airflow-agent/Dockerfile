FROM python:3.11-slim

# Keep your workdir as is
WORKDIR /app/airflow-agent

# Add this line to tell Python to include the current directory in its search path
ENV PYTHONPATH=/app/airflow-agent

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]