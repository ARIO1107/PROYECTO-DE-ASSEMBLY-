# BotMeter

BotMeter es una aplicación web que analiza comentarios de videos/publicaciones para estimar qué porcentaje son bots o cuentas automatizadas. Combina extracción de datos, ingeniería de características, heurísticas ponderadas y agregación para ofrecer un veredicto claro con trazabilidad.

## ¿Qué hace?

- **Análisis de YouTube**: Pega una URL pública de YouTube y analiza hasta 10.000 comentarios usando la YouTube Data API v3.
- **Análisis de otras redes sociales**: Sube un archivo CSV o JSON exportado (TikTok, Instagram, X, Facebook, Threads, Reddit, LinkedIn, etc.). El motor aplica exactamente las mismas heurísticas que para YouTube.
- **Motor heurístico transparente**: Extrae ~40 características por comentario y aplica 9 heurísticas ponderadas (spam/links, patrón de identidad, contenido genérico, ruido textual, temporal, formato, duplicación, emojis, desajuste de interacción).
- **Agregación con efecto clúster**: Detecta ráfagas, duplicados y aplica un amplificador de clúster.
- **Demo en vivo**: Simula un ataque de bots por Server-Sent Events sin necesidad de API keys.
- **Asistente "boti" (opcional)**: Resumen automático y chat con IA (Groq). Si no hay clave, la app sigue funcionando sin errores.

## Capturas

> [CAPTURA: Vista de bienvenida - selector YouTube / Otras redes]
> [CAPTURA: Resultados del análisis - veredicto, métricas, hallazgos]
> [CAPTURA: Comentarios sospechosos con razones]
> [CAPTURA: Demo en vivo (muro SSE)]

## Requisitos

- Python 3.12+
- Node.js (solo para recompilar Tailwind localmente si modificas el HTML)
- Claves opcionales:
  - `YOUTUBE_API_KEY` (YouTube Data API v3) - sin ella usa modo simulación
  - `GROQ_API_KEY` / `LLM_API_KEY` (Groq) - sin ella el asistente IA se desactiva

## Variables de entorno

Crea un archivo `.env` en la raíz (nunca lo subas al repo). Ejemplo sin valores reales:

```env
APP_NAME=BotMeter
APP_ENV=development
YOUTUBE_API_KEY=
LLM_API_KEY=
GROQ_API_KEY=
LLM_MODEL=openai/gpt-oss-20b
LLM_PROVIDER=groq
MAX_COMMENTS=10000
MAX_API_PAGES=100
API_QUOTA_PER_PAGE=1
SIMULATION_SALT=assembly-v1
```

Consulta `.env.example` para ver todas las opciones disponibles.

## Ejecutar en local

### Opción 1: Python directo

```bash
pip install -r requirements.txt
python app.py
```

La app estará en `http://localhost:8000`.

### Opción 2: uvicorn con recarga

```bash
pip install -r requirements.txt
uvicorn app:app --reload --host 127.0.0.1 --port 8000
```

### Recompilar Tailwind (si editas clases en `templates/index.html`)

```bash
npm install
npm run build
```

El CSS compilado (`static/tailwind.css`) va commiteado.

## Ejecutar con Docker

```bash
docker compose up --build
```

O directamente:

```bash
docker build -t botmeter .
docker run -p 8000:8000 --env-file .env botmeter
```

## Desplegar en Render

Usa el blueprint `render.yaml`:

1. Sube el repo a GitHub
2. En Render: `New` → `Blueprint` → selecciona el repositorio
3. Añade las claves secretas en `Environment > Add Secret`:
   - `YOUTUBE_API_KEY` (opcional)
   - `GROQ_API_KEY` (opcional)
4. Despliega

Render ejecuta `pip install -r requirements.txt` y arranca con `python app.py` (lee `$PORT` automáticamente).

## Advertencias importantes

- **Cuota de YouTube Data API v3**: `commentThreads.list` cuesta 1 unidad por llamada. Con valores por defecto puedes llegar rápido al límite diario (10.000 unidades/día). Si se agota la cuota, la app cae a modo simulación con avisos.
- **Cuota de Groq**: El asistente IA usa el tier gratuito de Groq. Si se agota, el robot pasa a modo mantenimiento sin romper la app.
- **Naturaleza heurística**: Los resultados son estimaciones orientativas, no verificaciones de identidad. Úsalos como señal para revisión manual.
- **Datos simulados**: Sin `YOUTUBE_API_KEY` se usan datos sintéticos deterministas. Las cifras sirven para validar la interfaz, no para tomar decisiones.
