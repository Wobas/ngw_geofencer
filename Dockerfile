FROM node:22 AS web-build

WORKDIR /build

COPY web/package*.json ./
RUN npm ci

COPY web ./
RUN npm run build

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir "GDAL==$(gdal-config --version)"

COPY geofencer ./geofencer
COPY --from=web-build /build/dist ./geofencer/static

WORKDIR /app/geofencer

EXPOSE 5000

CMD ["/usr/bin/supervisord", "-n", "-c", "/app/geofencer/supervisord.conf"]
