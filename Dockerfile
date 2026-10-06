FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -r requirements.txt
COPY tests ./tests
COPY data ./data
ENV PYTHONUNBUFFERED=1 DAGSTER_HOME=/var/lib/dagster
RUN mkdir -p /var/lib/dagster
CMD ["dagster", "dev", "-m", "datalake_demo.jobs.definitions", "-h", "0.0.0.0", "-p", "3000"]
