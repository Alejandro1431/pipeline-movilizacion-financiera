# Imagen del pipeline. No lleva datos dentro: el crudo se descarga al ejecutar,
# así que conviene montar un volumen en /app/data para que sobreviva al contenedor.
#
#   docker build -t pipeline-movilizacion-financiera .
#   docker run --rm -v "$PWD/data:/app/data" pipeline-movilizacion-financiera
#   docker run --rm -v "$PWD/data:/app/data" pipeline-movilizacion-financiera perfil
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Las dependencias van primero para que la capa se reutilice mientras no cambien.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["all"]
