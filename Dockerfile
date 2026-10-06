# ---------------------------------------------------------------------------
# Project Assembly - imagen de produccion para FastAPI
# ---------------------------------------------------------------------------
# Multi-stage: la etapa `builder` compila/instala dependencias y la etapa final
# copia solo lo necesario. Reduce el tamano final y minimiza superficie de ataque.

# ---------- Etapa 1: instalacion de dependencias ---------------------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.txt .

# Compila las ruedas (wheels) en un virtualenv para copiarlas luego.
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install -r requirements.txt


# ---------- Etapa 2: imagen final minima -----------------------------------
FROM python:3.12-slim AS final

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8 \
    PATH="/opt/venv/bin:$PATH" \
    APP_ENV=production \
    PORT=8000

WORKDIR /app

# Dependencias ya resueltas desde la etapa builder
COPY --from=builder /opt/venv /opt/venv

# Usuario no root: buena practica de seguridad en contenedores.
# Se crea ANTES del COPY para que `--chown` pueda resolver el propietario.
RUN groupadd --system --gid 10001 appgroup \
    && useradd --system --uid 10001 --gid appgroup --no-create-home appuser

# Codigo de la aplicacion (respeta .dockerignore)
COPY --chown=appuser:appuser app.py ./
COPY --chown=appuser:appuser templates ./templates

USER appuser

EXPOSE 8000

# Healthcheck sin depender de curl: usa la libreria estandar de Python.
# Forma de shell a proposito, para que ${PORT} se expanda en tiempo de ejecucion.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import os,urllib.request,sys; url='http://127.0.0.1:%s/health' % os.environ.get('PORT','8000'); sys.exit(0 if urllib.request.urlopen(url, timeout=3).status == 200 else 1)"

# `exec` para que uvicorn reciba las senales del contenedor (SIGTERM) y
# cierre los jobs en curso de forma ordenada.
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*'"]