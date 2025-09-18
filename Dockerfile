FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create logs directory
RUN mkdir -p logs && chmod 755 logs

# Create startup script to run both scheduler and HTTP server
RUN echo '#!/bin/bash\necho "Starting HedgeFund Agent..."\necho "Starting scheduler with integrated HTTP server..."\npython scheduler.py' > start.sh && chmod +x start.sh

EXPOSE 3002

CMD ["./start.sh"]