# BotMeter — instrucciones de trabajo

## Git (obligatorio)
- **Siempre commitear y pushear todos los cambios** al terminar una tarea.
  - `git add <archivos> && git commit -m "<mensaje>" && git push origin main`
- Antes de commitear: `git status` y `git diff` para revisar; nunca subir secretos
  (`.env` ya está en `.gitignore`).
- Mensajes en español, concisos, siguiendo el estilo del historial.
- No dejar archivos temporales en el repo (los de verificación van en
  `C:\Users\Usuario\AppData\Local\Temp\opencode\`).

## Proyecto
- Backend: `app.py` (FastAPI + uvicorn, Jinja2). Frontend: `templates/index.html`
  (SPA, Tailwind Play CDN, sin build step).
- Estética: dark corporativo, fondo `#1a1613`, acento naranja `#ff5c35` (paleta
  `brand`), textos secundarios `stone`. Sin emojis como iconos.
- IA "Wardo" (Groq, `openai/gpt-oss-20b`): clave solo en `.env` local o secret de
  Render; degradación obligatoria a modo mantenimiento (nunca 500).

## Verificación
- `python -m py_compile app.py`
- `node --check` sobre el JS extraído (script `check_ui.py` en Temp).
- Servir en un puerto libre y comprobar la respuesta real antes del commit.
