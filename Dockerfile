FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN rm -f foerdermittel.db foerdermittel.db-shm foerdermittel.db-wal foerdermittel.db.tmp

RUN mkdir -p /data
ENV DB_PATH=/data/foerdermittel.db

EXPOSE 8080

CMD ["python", "mcp_server.py", "--transport", "sse", "--port", "8080"]
