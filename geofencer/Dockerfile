FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    supervisor \
    gdal-bin \
    libgdal-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

COPY .env .

EXPOSE 5000

CMD ["/usr/bin/supervisord", "-n"]
