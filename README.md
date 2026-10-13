# SocialBotGuard

Escáner y **simulador de moderación en tiempo real**. Genera un flujo de comentarios (o te deja
escribirlos a mano), puntúa cada mensaje con tres heurísticas ponderadas y marca en vivo los que
parecen de un bot: `[BOT DETECTADO - X% Confianza]`.

Todo el análisis ocurre **en memoria y sobre datos simulados**: no se conecta a ninguna red social,
no usa API keys y no toca datos reales. FastAPI + WebSockets + Tailwind (CDN). Sin build step y
desplegable en Render o Docker.

---

## Qué hace

- **Motor de 3 reglas** con pesos fijos y umbral de similitud configurable en vivo.
- **Pools dinámicos de comentarios**: perfiles humanos variados (saludos, modismos, preguntas,
  emojis) y bots sofisticados con variaciones sutiles del mismo mensaje. La generación usa una
  "bolsa" que reparte al azar sin repetir hasta agotar el banco.
- **Flujo en vivo por WebSocket**: el servidor empuja cada comentario y sus KPIs al panel.
- **Controles en vivo (sliders)**: umbral de similitud (60–90 %) e intensidad del flujo
  (0,25x–3x) se ajustan en tiempo real.
- **Control total desde la interfaz**: iniciar/pausar, *Simular Ataque de Bots*, enviar un
  comentario manual y reiniciar el muro.
- **Lista negra**: bloqueo y **desbloqueo** de cuentas; las bloqueadas dejan de publicar.

### Reglas del motor

| Regla | Peso | Condición |
|---|---|---|
| Frecuencia | +40 % y progresiva | Ventana deslizante: más de 3 comentarios en 5 s. Cada mensaje extra suma +10 % (tope +60 %) y a partir de 5 se activa una **alerta roja de spam** |
| Similitud | +50 % | `difflib.SequenceMatcher` **o** Jaccard por encima del umbral (por defecto 0.80, ajustable 0.60–0.90) |
| Patrones | +20 % | Varios enlaces en un mensaje o mayúsculas sostenidas |

Si el riesgo acumulado llega a **70 % o más**, el mensaje se marca como `[BOT DETECTADO]` con su
razón (por ejemplo, «Ráfaga de 6 mensajes en menos de 5 s» o «Alta similitud de texto (Seq 92 % /
Jaccard 100 %)»). El riesgo se acota a 100 %.

---

## Puesta en marcha

```bash
python -m pip install -r requirements.txt
python app.py                    # lee $PORT (por defecto 8000)
```

En desarrollo, con recarga en caliente:

```bash
set APP_ENV=development
uvicorn app:app --reload
```

Abre <http://localhost:8000>.

### Variables de entorno

Todas son opcionales; la app arranca sin ninguna.

| Variable | Para qué | Por defecto |
|---|---|---|
| `PORT` | Puerto de escucha (lo inyectan Render/Railway/Docker) | `8000` |
| `APP_ENV` | `development` activa `--reload`; cualquier otro valor, producción | `production` |
| `APP_NAME` | Nombre mostrado en el panel | `SocialBotGuard` |
| `APP_VERSION` | Versión mostrada y servida en `/health` | `2.0.0` |

---

## Uso

1. **Iniciar flujo**: emite comentarios de usuarios simulados (~18 % fraudulentos).
2. **Simular Ataque de Bots**: ráfaga de mensajes promocionales desde pocas cuentas, que dispara la
   regla de frecuencia, la alerta roja de spam y la detección al instante.
3. **Mensaje manual**: escribe un comentario y observa cómo lo puntúa el motor.
4. **Sensibilidad (sliders)**: ajusta el umbral de similitud y la intensidad del flujo; se aplican
   en vivo y afectan al análisis inmediatamente.
5. **Bloquear / Desbloquear Cuenta**: sobre cualquier bot detectado o desde la **Lista negra** del
   panel derecho; una cuenta bloqueada deja de publicar.
6. **Reiniciar muro**: vacía el historial, pone los contadores a cero y deja el flujo en pausa
   (conserva tu configuración de los sliders).

### API

| Método y ruta | Descripción |
|---|---|
| `GET /` | Panel de control (dashboard de 3 paneles) |
| `GET /health` | Liveness probe (`{"status": "ok", "version": ...}`) |
| `GET /api/config` | Config actual, rangos de los sliders y pesos del motor |
| `GET /api/stats` | Foto actual de los contadores y de la lista negra |
| `WS /ws` | Canal bidireccional del flujo en vivo |
| `GET /docs` | Documentación interactiva (Swagger UI) |

**Protocolo WebSocket.** El cliente envía JSON con un campo `type`:

```jsonc
{ "type": "control", "action": "start" | "pause" | "attack" | "reset", "size": 16 }
{ "type": "manual",  "user": "invitado", "text": "mi comentario" }
{ "type": "block",   "user": "promo_deals_24" }
{ "type": "unblock", "user": "promo_deals_24" }
{ "type": "config",  "similarity_threshold": 0.75, "stream_speed": 1.5 }
```

El servidor emite JSON con un campo `type` entre `snapshot`, `message`, `stats`, `state`, `config`,
`blocked`, `unblocked`, `reset` y `error`. Al conectar, el primer mensaje es un `snapshot` con el
estado, la config y los últimos comentarios. Los valores de `config` se recortan al rango válido.

---

## Tests

```bash
python -m pip install pytest
python -m pytest -q
```

19 tests cubren las tres reglas por separado, la similitud de SequenceMatcher + Jaccard, la
frecuencia por ventana deslizante con penalización progresiva y alerta de spam, el tope de riesgo,
la configuración en vivo (con recorte de rango) y la lista negra con desbloqueo. Usan tiempos
controlados (sin `sleep`), así que son deterministas.

Además, un probe de extremo a extremo (`httpx` + `websockets`) verifica el handshake, un ataque de
bots con alerta de ráfaga, bloqueo y desbloqueo, un mensaje manual, el cambio de config en vivo y el
reset contra un servidor real.

---

## Arquitectura

```
app.py
  1. Reglas puras          count_urls · is_uppercase_blast · text_similarity · jaccard_similarity
  2. Estado y veredicto    UserState · Verdict · Message
  3. Motor                 BotEngine.analyze() -> Verdict (riesgo, es_bot, reglas, razón, similitud, alerta)
  4. Pools dinamicos       ShuffleBag: HUMAN_NAMES/COMMENTS y BOT_NAMES/MESSAGES
  5. Hub                   ModerationHub: estado, config en vivo, clientes WS y bucle del simulador
  6. FastAPI               GET / , /health , /api/config , /api/stats , WS /ws
templates/index.html      dashboard de 3 paneles + sliders + lista negra + cliente WebSocket nativo
```

El estado vive en un único objeto `ModerationHub` en memoria: el bucle del simulador es la única
vía de publicación, todos los clientes WebSocket reciben el mismo flujo y la configuración de los
sliders afecta al motor al instante.

---

## Despliegue

```bash
docker compose up --build      # http://localhost:8000
```

El `Dockerfile` es multi-stage, corre como usuario no root e incluye `HEALTHCHECK`. En Render,
`render.yaml` usa el runtime de Python y arranca con `python app.py` (que activa `--proxy-headers`,
necesario también para las conexiones WebSocket detrás del proxy).

---

## Limitaciones conocidas

1. **No hay datos reales.** Es un simulador: el «tráfico» lo genera el propio servidor y las
   heurísticas no están calibradas contra un corpus etiquetado. Las cifras son orientativas.
2. **El léxico simulado es finito** (aunque amplio y con variaciones): los bots y humanos salen de
   los bancos `HUMAN_*` / `BOT_*`, así que las demos lucen mejor de lo que rendiría el motor con
   texto arbitrario.
3. **El estado es por proceso y volátil.** Un reinicio del servidor (o varias réplicas) parte de
   cero; no hay persistencia ni estado compartido entre instancias.
