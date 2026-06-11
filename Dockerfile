FROM python:3.11-slim
# TikTok End-to-End Crawler — Docker image

# ---- Install system dependencies ----
RUN apt-get update && apt-get install -y \
    bash \
    curl \
    gcc \
    g++ \
    make \
    && apt-get clean

# Set timezone to Asia/Jakarta
RUN cp /usr/share/zoneinfo/Asia/Jakarta /etc/localtime \
    && echo "Asia/Jakarta" > /etc/timezone

# Upgrade pip
RUN pip install --upgrade pip

# ---- Install Python dependencies ----
COPY source/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ---- Copy application code ----
COPY config.yaml /app/config.yaml
COPY source/ /app
WORKDIR /app

EXPOSE 9092

# Default: print help; override with CLI args at runtime
ENTRYPOINT ["python", "main.py"]
CMD ["crawler", "--help"]
