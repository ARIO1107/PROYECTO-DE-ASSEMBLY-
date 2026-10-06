"""
Project Assembly - MVP de deteccion de bots en comentarios de redes sociales
=============================================================================

Arquitectura
------------
    app.py            Single-module service (facil de auditar y desplegar)
    templates/        Interfaz web minima (Jinja2 + Tailwind por CDN)
    requirements.txt  Dependencias fijadas
    Dockerfile        Imagen multi-stage, usuario no root
    docker-compose.yml Entorno de desarrollo local

El pipeline de analisis tiene 4 fases desacopladas y testeables por separado:

    1. EXTRACCION   -> `resolve_video()` + `fetch_comments()`
    2. INGENIERIA    -> `build_features()` (features crudas por comentario)
    3. PUNTUACION    -> `RULES` (10 heuristicas ponderadas, 0..1 por regla)
    4. AGREGACION   -> `aggregate()` (pandas) -> respuesta JSON

Diseno del scoring (importante)
-------------------------------
Cada comentario obtiene un score 0..100 = suma ponderada de las heuristicas.
El score NO se usa como veredicto absoluto: es una senal heuristica orientativa
que debe interpretarse junto con las metricas de contexto del canal.
El agregado aplica ademas un "efecto cluster" porque los bots suelen llegar en
ráfagas y de forma duplicada, no de manera independiente.
"""

from __future__ import annotations

import math
import random
import re
import statistics
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Sequence
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import AliasChoices, BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"


# ===========================================================================
# 1. CONFIGURACION
# ===========================================================================

class Settings(BaseSettings):
    """Configuracion leida de variables de entorno (o de un fichero .env).

    Los alias se declaran explicitamente: sin ellos, un campo llamado
    `environment` leeria la variable ENVIRONMENT y el `APP_ENV` del Dockerfile
    se ignoraria en silencio.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    app_name: str = Field(default="Project Assembly", validation_alias=AliasChoices("APP_NAME", "app_name"))
    app_version: str = Field(default="1.0.0", validation_alias=AliasChoices("APP_VERSION", "app_version"))
    environment: str = Field(
        default="development",
        validation_alias=AliasChoices("APP_ENV", "ENVIRONMENT"),
    )

    # YouTube Data API v3. Sin clave la app entra en MODO SIMULACION.
    youtube_api_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("YOUTUBE_API_KEY", "youtube_api_key"),
    )

    # Limites de recogida del dataset por analisis.
    max_comments: int = Field(default=120, ge=10, le=300)
    request_timeout: float = Field(default=10.0, gt=0, le=60)

    # Semilla del simulador: cambiarla cambia los datos falsos generados.
    simulation_salt: str = Field(default="assembly-v1", validation_alias=AliasChoices("SIMULATION_SALT", "simulation_salt"))


settings = Settings()

YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/commentThreads"
YOUTUBE_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"


# ===========================================================================
# 2. MODELOS (Pydantic v2) - contrato de entrada / salida de la API
# ===========================================================================

class AnalyzeRequest(BaseModel):
    """Payload de POST /api/analyze."""

    url: str = Field(
        ...,
        min_length=4,
        max_length=2048,
        description="URL de YouTube (watch, youtu.be, /shorts, /embed, /live) o ID de video.",
        examples=["https://www.youtube.com/watch?v=dQw4w9WgXcQ"],
    )
    comment_limit: Optional[int] = Field(
        default=None,
        ge=10,
        le=300,
        description="Numero maximo de comentarios a analizar (por defecto: settings.max_comments).",
    )
    include_comments: bool = Field(
        default=True,
        description="Incluir el detalle por comentario en la respuesta.",
    )

    @field_validator("url")
    @classmethod
    def _strip(cls, v: str) -> str:
        return v.strip()


class AnalyzeResponse(BaseModel):
    """Respuesta completa del analisis (documentacion autogenerada en /docs)."""

    analysis_id: str
    generated_at: str
    app_version: str
    dataset_source: str                      # "youtube_api" | "simulated"
    source: dict[str, Any]                    # video_id, url canonica, titulo, canal
    metrics: dict[str, Any]                   # KPI agregados
    risk: dict[str, Any]                      # nivel + interpretacion
    distribution: dict[str, int]              # alto / medio / bajo
    signals: list[dict[str, Any]]              # heuristicas que mas pesan
    feature_averages: dict[str, float]        # medias de las features clave
    findings: list[str]                       # conclusiones legibles
    comments: list[dict[str, Any]]            # detalle por comentario
    warnings: list[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str
    version: str
    environment: str
    data_mode: str


# ===========================================================================
# 3. EXTRACCION: normalizacion de URLs de YouTube
# ===========================================================================

VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
YOUTUBE_HOSTS = {
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "music.youtube.com", "youtu.be", "www.youtu.be", "youtube-nocookie.com",
}


def resolve_video(raw: str) -> str:
    """Extrae y valida el ID de 11 caracteres de una URL o ID de YouTube.

    Acepta:
        https://www.youtube.com/watch?v=ID
        https://youtu.be/ID?t=30
        https://www.youtube.com/shorts/ID
        https://www.youtube.com/embed/ID
        https://www.youtube.com/live/ID
        ID pelado
    """
    value = raw.strip()

    # Caso 1: ya es un ID.
    if VIDEO_ID_RE.match(value):
        return value

    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = (parsed.hostname or "").lower()

    if host not in YOUTUBE_HOSTS:
        raise HTTPException(
            status_code=400,
            detail=f"Host no soportado: '{host or value}'. Este MVP solo analiza YouTube.",
        )

    segments = [s for s in parsed.path.split("/") if s]
    query = parse_qs(parsed.query)

    # Caso 2: /watch?v=ID
    if segments and segments[0] == "watch":
        candidate = (query.get("v") or [""])[0]
        if candidate:
            return _validate_video_id(candidate, value)

    # Caso 3: /shorts/ID, /embed/ID, /live/ID, /v/ID
    if segments and segments[0] in {"shorts", "embed", "live", "v"} and len(segments) > 1:
        return _validate_video_id(segments[1], value)

    # Caso 4: youtu.be/ID
    if host.endswith("youtu.be") and segments:
        return _validate_video_id(segments[0], value)

    raise HTTPException(
        status_code=400,
        detail="No se pudo extraer un ID de video. Formatos validos: /watch?v=ID, youtu.be/ID, /shorts/ID.",
    )


def _validate_video_id(candidate: str, original: str) -> str:
    if VIDEO_ID_RE.match(candidate):
        return candidate
    raise HTTPException(status_code=400, detail=f"ID de video invalido en la URL: '{original}'.")


def canonical_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


# ===========================================================================
# 4. EXTRACCION: obtencion de comentarios (API real o simulacion)
# ===========================================================================

@dataclass
class RawComment:
    """Comentario normalizado, sea real o simulado."""

    comment_id: str
    author: str
    text: str
    published_at: Optional[datetime]
    like_count: Optional[int]
    reply_count: Optional[int]
    has_channel: bool                  # el usuario tiene canal propio
    is_author: bool = False             # es el creador del video
    depth: int = 0                     # 0 = principal, 1 = respuesta


@dataclass
class Dataset:
    comments: list[RawComment]
    source: str                        # "youtube_api" | "simulated"
    warnings: list[str] = field(default_factory=list)
    video_meta: dict[str, Any] = field(default_factory=dict)


def _http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": f"{settings.app_name}/{settings.app_version}"})
    return session


def fetch_youtube_comments(video_id: str, limit: int) -> Dataset:
    """Descarga comentarios reales con la YouTube Data API v3 (commentThreads.list).

    Maneja los errores esperables: sin clave, cuota agotada, comentarios
    deshabilitados o video inexistente. En cualquiera de ellos cae en modo
    simulacion para que el MVP siga siendo navegable.
    """
    if not settings.youtube_api_key:
        ds = simulate_comments(video_id, limit)
        ds.warnings.append(
            "YOUTUBE_API_KEY no configurada: se Sirvio un dataset SIMULADO con fines de demo. "
            "Define la variable para analizar comentarios reales."
        )
        return ds

    session = _http_session()
    collected: list[RawComment] = []
    warnings: list[str] = []
    page_token: Optional[str] = None
    meta: dict[str, Any] = {}
    hard_error: Optional[str] = None

    # Pagina hasta agotar el presupuesto de comentarios o 5 paginas (cuota).
    for _ in range(5):
        if len(collected) >= limit:
            break
        params: dict[str, Any] = {
            "part": "snippet,replies(snippet)",
            "videoId": video_id,
            "maxResults": 100,
            "order": "relevance",
            "textFormat": "plainText",
            "key": settings.youtube_api_key,
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            response = session.get(YOUTUBE_API_URL, params=params, timeout=settings.request_timeout)
        except requests.RequestException as exc:            # timeout / DNS / TLS
            hard_error = f"Error de red al contactar YouTube: {exc}"
            break

        if response.status_code != 200:
            payload = response.json() if response.content else {}
            error = payload.get("error", {})
            reason = error.get("errors", [{}])[0].get("reason", "UNKNOWN")
            message = error.get("message", response.text[:200])
            hard_error = f"YouTube API {response.status_code} ({reason}): {message}"
            break

        for thread in response.json().get("items", []):
            snippet = thread.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
            collected.append(_parse_api_comment(snippet, depth=0))
            for reply in thread.get("replies", {}).get("comments", []):
                collected.append(_parse_api_comment(reply.get("snippet", {}), depth=1))

        page_token = response.json().get("nextPageToken")
        if not page_token:
            break

    if hard_error:
        ds = simulate_comments(video_id, limit)
        ds.warnings.append(f"Fallo la API real ({hard_error}); se genero un dataset SIMULADO.")
        return ds

    if not collected:
        ds = simulate_comments(video_id, limit)
        ds.warnings.append(
            "YouTube no devolvio comentarios (posiblemente desactivados o video sin comentarios); "
            "se genero un dataset SIMULADO."
        )
        return ds

    if not meta:
        meta = _fetch_video_meta(session, video_id, warnings)

    return Dataset(
        comments=collected[:limit],
        source="youtube_api",
        warnings=warnings,
        video_meta=meta,
    )


def _parse_api_comment(snippet: dict[str, Any], depth: int) -> RawComment:
    """Traduce el snippet de la API a nuestro modelo interno."""
    published: Optional[datetime] = None
    raw_date = snippet.get("publishedAt")
    if raw_date:
        try:
            published = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        except ValueError:
            published = None

    return RawComment(
        comment_id=snippet.get("id", uuid.uuid4().hex[:12]),
        author=snippet.get("authorDisplayName") or "@desconocido",
        text=snippet.get("textOriginal") or snippet.get("textDisplay") or "",
        published_at=published,
        like_count=int(snippet.get("likeCount") or 0),
        reply_count=int(snippet.get("replyCount") or snippet.get("totalReplyCount") or 0),
        has_channel=bool(snippet.get("authorChannelId")),
        is_author=bool(snippet.get("authorIsChannelOwner")),
        depth=depth,
    )


def _fetch_video_meta(session: requests.Session, video_id: str, warnings: list[str]) -> dict[str, Any]:
    """Metadatos del video (titulo y canal). Degradado: no es critico."""
    try:
        response = session.get(
            YOUTUBE_VIDEOS_URL,
            params={
                "part": "snippet,statistics",
                "id": video_id,
                "key": settings.youtube_api_key,
            },
            timeout=settings.request_timeout,
        )
        if response.status_code == 200:
            items = response.json().get("items", [])
            if items:
                snippet = items[0].get("snippet", {})
                stats = items[0].get("statistics", {})
                return {
                    "title": snippet.get("title"),
                    "channel": snippet.get("channelTitle"),
                    "channel_id": snippet.get("channelId"),
                    "published_at": snippet.get("publishedAt"),
                    "view_count": int(stats.get("viewCount") or 0),
                    "like_count": int(stats.get("likeCount") or 0),
                    "comment_count": int(stats.get("commentCount") or 0),
                }
        warnings.append("No se pudieron obtener los metadatos del video (no afecta al analisis).")
    except requests.RequestException:
        warnings.append("Metadatos del video no disponibles por error de red (no afecta al analisis).")
    return {}


# --- 4.1 Simulador determinista -------------------------------------------
# Objetivo: generar un dataset plausible y REPRODUCIBLE (mismo video -> mismo
# resultado) para poder desarrollar y testear sin depender de la API ni de su
# cuota. La mezcla bot/humano se genera con una semilla derivada del video ID.

_HUMAN_TEMPLATES = [
    "{op} al final del video, muy buen resumen",
    "a los {n} minutos es cuando se pone interesante de verdad",
    "el audio se escucha mal en mi movil, alguien mas tiene el mismo problema?",
    "no estoy de acuerdo con lo que dices sobre {topic}, pero el video esta muy bien hecho",
    "lo vi hace tiempo y sigue siendo de lo mejor que hay de {topic}",
    "muchas gracias por la info, me sirvio bastante",
    "parte 2 porfa, hay mucho mas que contar sobre esto",
    "el algoritmo de youtube me trajo aqui y me ha gustado mucho",
    "vengo de otro video tuyo y este esta infinitamente mejor explicado",
    "{op} comentario fijo en cada video, saludos",
    "se nota que te tomaste tu tiempo en esto, se agradece",
    "cuesta entenderlo al principio pero luego tiene mucho sentido",
    "tengo una duda: como hiciste para grabar eso sin equipo?",
    "el final me dejo pensando mucho, no lo esperaba",
    "Otro video mas de {topic} y aun asi sigue manteniendo el interes",
]

_HUMAN_OPINIONS = [
    "totalmente de acuerdo", "no estoy seguro", "me parece una buena idea",
    "lo dudo mucho", "claro que si", "pues mira tu",
]

_HUMAN_TOPICS = [
    "este tema", "el algoritmo", "la musica", "el cine", "la ciencia",
    "los juegos", "la fotografia", "la historia", "el analisis",
]

_HUMAN_SHOUTS = ["BUEN VIDEO!!", "QUE LOCURA", "ME ENCANTO ESTO 🔥", "TOP 1", "INCREIBLE!!!!"]

_BOT_TEXT_TEMPLATES = [
    "GANA DINERO DESDE CASA 👉 {link} promocion limitada hoy!!!",
    "crypto gratis 100% ((((({link})))) NFT airdrop activo ahora",
    "OnlyFans mi pagina 😈😈😈 -{link}-.curated gratis",
    "telegram contacto directo -{link}- grupo privado FX signals",
    "CASINO BONUS 200% DEPOSIT {link} sin requisitos REGISTRO URGENTE",
    "mi canal tiene los mejores remix 2026 ►{link}► NO TE LO PIERDAS",
    "sigueloAqui_RETRO 2024 2025 2026 historial completo -{link}- MEJORAS HD 4K",
    "click here now {link} free download crack apk mod premium unlocked",
    "Trabaja desde casa gana 500USD diarios escribeme {link} oportunidades",
    "🔥🔥🔥 OFERTA IMPERDIBLE 🔥🔥🔥 -{link}- ENVIO GRATIS TODO EL PAIS",
    "MIRA ESTO QUE ENCONTRE {link} wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww",
    "hola a todos saludos a mi familia y a los amigos 🔥🔥 me gusta mucho este video",
]

_BOT_SHORT_SPAM = [
    "SUSCRIBETE!!! 🔥🔥🔥🔥",
    "KJSDFHKSJDFHSKDJFHSKDJFHSKDFJHSKDJFHSKDJFH",
    "MIRA MI VIDEO https://spam.example/promo",
    "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥",
    "AAAAAAAAAAAAA MIRA ESTO",
    "+34 6XX XXX XXX compruebalo ya",
]

_BOT_USERNAMES = [
    "user83920184", "xg_88231_top", "MegaPromo2026", "channel_oficial_2024",
    "tXt0pPr0m0", "user47291827364", "FREEMONEY_24", "a1b2c3d4e5f6",
    "PromoOfficialVideoHD", "sexy_hot_2026",
]

_HUMAN_USERNAMES = [
    "Lucia M.", "dev_ramos", "marta.gp", "NightOwl_92", "Carlos YT fan",
    "ana.perez", "Fran", "TheRealSonia", "juan.10", "Kai",
    "Vale Mtz", "curious.cat",
]

_VIDEO_TITLES = [
    "Como funciono un modelo de lenguaje en 20 minutos",
    "El error de analisis que todos cometemos",
    "Probé 30 auriculares economicos - estos son los ganadores",
    "Historia completa del synth pop (1960-2026)",
    "Mi setup de escritorio por menos de 300 EUR",
    "React Native vs Flutter - comparativa real de 2026",
]


def simulate_comments(video_id: str, limit: int) -> Dataset:
    """Genera un dataset sintetico pero verosimil, deterministico por video_id."""
    rng = random.Random(f"{settings.simulation_salt}:{video_id}")

    # Mezcla variable de bots por video: el MVP debe mostrar resultados distintos.
    bot_ratio = rng.uniform(0.18, 0.62)
    total = min(limit, rng.randint(max(24, limit // 2), limit))
    bot_count = int(total * bot_ratio)
    human_count = total - bot_count

    base_time = datetime.now(timezone.utc) - timedelta(days=rng.randint(1, 9))
    comments: list[RawComment] = []

    # --- Comentarios humanos: distribucion realista (muchos cortos, pocos largos)
    for _ in range(human_count):
        text = _render_human_text(rng)
        has_channel = rng.random() < 0.35
        comments.append(
            RawComment(
                comment_id=rng.getrandbits(48).to_bytes(6, "big").hex(),
                author=rng.choice(_HUMAN_USERNAMES) + ("" if rng.random() < 0.5 else str(rng.randint(1, 99))),
                text=text,
                published_at=base_time + timedelta(minutes=rng.randint(0, 60 * 72)),
                like_count=max(0, int(rng.lognormvariate(1.2, 1.4))),
                reply_count=0 if rng.random() < 0.8 else rng.randint(1, 4),
                has_channel=has_channel,
                is_author=False,
                depth=0 if rng.random() < 0.8 else 1,
            )
        )

    # --- Comentarios bot: spam por palabras clave, enlaces, rafagas y duplicados
    for index in range(bot_count):
        # Un 18% de los bots reutiliza exactamente el mismo texto (red de spam).
        if rng.random() < 0.18:
            text = _BOT_TEXT_TEMPLATES[0].format(link=f"https://promo-{rng.randint(1, 4)}.example/x")
        elif rng.random() < 0.28:
            text = rng.choice(_BOT_SHORT_SPAM)
        else:
            text = _render_bot_text(rng)

        # Rafaga: 2-5 comentarios del mismo autor casi simultaneos.
        burst = rng.random() < 0.25
        if burst:
            minute_offset = rng.randint(0, 60 * 72)
            author = rng.choice(_BOT_USERNAMES)
            for sub in range(rng.randint(2, 5)):
                comments.append(
                    RawComment(
                        comment_id=rng.getrandbits(48).to_bytes(6, "big").hex(),
                        author=author,
                        text=text,
                        published_at=base_time + timedelta(minutes=minute_offset, seconds=sub * rng.randint(2, 25)),
                        like_count=0,
                        reply_count=0,
                        has_channel=False,
                        is_author=False,
                        depth=0,
                    )
                )
            continue

        comments.append(
            RawComment(
                comment_id=rng.getrandbits(48).to_bytes(6, "big").hex(),
                author=rng.choice(_BOT_USERNAMES),
                text=text,
                published_at=base_time + timedelta(minutes=rng.randint(0, 60 * 72)),
                like_count=0,
                reply_count=0,
                has_channel=rng.random() < 0.15,
                is_author=False,
                depth=0,
            )
        )

    comments.sort(key=lambda c: c.published_at or base_time)
    return Dataset(
        comments=comments,
        source="simulated",
        video_meta={
            "title": rng.choice(_VIDEO_TITLES),
            "channel": f"Canal Demo {rng.randint(1, 40)}",
            "channel_id": f"UC{rng.getrandbits(30):010d}",
            "published_at": base_time.isoformat(),
            "view_count": rng.randint(4_000, 4_000_000),
            "like_count": rng.randint(200, 400_000),
            "comment_count": len(comments),
        },
    )


def _render_human_text(rng: random.Random) -> str:
    roll = rng.random()
    if roll < 0.12:
        return rng.choice(_HUMAN_SHOUTS)
    if roll < 0.42:
        return rng.choice(["ok", "nice", "+1", "jajaja", "🔥", "xd", "de nada", "👍", "meToo", "esta bien"]) + \
            (" " + rng.choice(_HUMAN_TOPICS) if rng.random() < 0.4 else "")
    if roll < 0.72:
        return rng.choice(_HUMAN_TEMPLATES).format(
            op=rng.choice(_HUMAN_OPINIONS),
            n=rng.randint(2, 59),
            topic=rng.choice(_HUMAN_TOPICS),
        )
    return " ".join(rng.choice(_HUMAN_TOPICS + _HUMAN_OPINIONS) for _ in range(rng.randint(12, 45)))


def _render_bot_text(rng: random.Random) -> str:
    template = rng.choice(_BOT_TEXT_TEMPLATES)
    text = template.format(link=f"https://bit.ly/{rng.getrandbits(32):08x}")
    # Ruido tipico: elongation de caracteres y exclamaciones multiples.
    if rng.random() < 0.35:
        text += " " * 1 + rng.choice(["!!!", "!!!!", "!!!!!", "🔥🔥🔥🔥"])
    if rng.random() < 0.20:
        text = text.replace(" ", rng.choice([" * ", "  ", "\n"]))
    return text


# ===========================================================================
# 5. INGENIERIA DE FEATURES
# ===========================================================================

EMOJI_RE = re.compile(
    "["
    "\U0001F1E0-\U0001FAFF"    # emoji y banderas
    "\U00002600-\U000027BF"    # simbolos, dingbats, flechas
    "\U0001F000-\U0001F0FF"    # supplementos
    "\U00002B00-\U00002BFF"
    "\U0000FE0F"               # selector de variacion
    "\U0000203C"
    "\U00002049"
    "]",
    flags=re.UNICODE,
)
URL_RE = re.compile(r"(https?://|www\.)\S+", re.IGNORECASE)
REPEAT_CHARS_RE = re.compile(r"(.)\1{3,}")
ALLCAPS_WORD_RE = re.compile(r"\b[A-ZÁÉÍÓÚÑ]{4,}\b")
WORD_RE = re.compile(r"[\w'\u00C0-\u024F]+", re.UNICODE)

# Lexicon de senales de spam (ES/EN). Peso por termino, no solo presencia.
SPAM_LEXICON: dict[str, float] = {
    "gratis": 0.30, "gana dinero": 0.90, "ganar dinero": 0.90, "dinero facil": 0.95,
    "trabaja desde casa": 0.85, "airdrop": 0.95, "crypto": 0.70, "nft": 0.80,
    "casino": 0.90, "apuestas": 0.75, "apuesta": 0.70, "bonus": 0.55,
    "onlyfans": 0.95, "telegram": 0.70, "whatsapp": 0.75, "seguidme": 0.35,
    "suscribete": 0.25, "clic aqui": 0.70, "click aqui": 0.70, "link en bio": 0.60,
    "descarga": 0.45, "crack": 0.85, "apk": 0.60, "mod": 0.50, "premium": 0.35,
    "promocion": 0.55, "oferta": 0.50, "descuento": 0.40, "regalo": 0.45,
    "hot girls": 0.80, "sexy": 0.55, "privado": 0.35, "contacto directo": 0.60,
    "mejoras hd": 0.70, "4k": 0.45, "siguelo aqui": 0.75, "miralo aqui": 0.65,
    "fx signals": 0.85, "invitacion": 0.50, "gratis de verdad": 0.90, "unete": 0.40,
}

# Plantillas de elogio generico: alta frecuencia, baja informacion semantica.
GENERIC_PRAISE_RE = re.compile(
    r"\b("
    r"buen[oa]s video|buen[oa]s videos|muy bien hecho|excelente|increible|genial|"
    r"buenisimo|me encanto|lo mejor|best video|great video|very good|nice video|"
    r"buen canal|gran canal|felicidades|felicidades por el video"
    r")\b",
    re.IGNORECASE,
)

STOPWORDS = {
    # espanol
    "de", "la", "que", "el", "en", "los", "las", "del", "se", "las", "un", "por", "con", "no", "una",
    "su", "para", "es", "al", "lo", "como", "mas", "pero", "sus", "le", "ya", "o", "este", "si",
    "porque", "esta", "cuando", "muy", "sin", "sobre", "tambien", "me", "hasta", "hay", "donde",
    "quien", "todo", "te", "tu", "lo", "le", "yo", "va", "ha", "sino", "pues", "ser", "ha",
    # ingles
    "the", "and", "you", "that", "this", "with", "for", "not", "are", "was", "but", "have", "has",
    "they", "from", "would", "there", "their", "what", "about", "which", "when", "video", "channel",
}


@dataclass
class CommentFeatures:
    """Features crudas (todas en unidades fisicas, sin normalizar)."""

    comment_id: str
    author: str
    text: str
    text_preview: str
    published_at: Optional[str]
    depth: int
    is_author: bool

    text_length: int = 0
    word_count: int = 0
    unique_word_ratio: float = 0.0
    avg_word_length: float = 0.0
    stopword_ratio: float = 0.0
    vowel_ratio: float = 0.0
    char_entropy: float = 0.0
    emoji_count: int = 0
    emoji_density: float = 0.0
    link_count: int = 0
    digit_ratio: float = 0.0
    uppercase_ratio: float = 0.0
    allcaps_words: int = 0
    exclamation_count: int = 0
    repeated_char_groups: int = 0
    question_count: int = 0
    non_latin_ratio: float = 0.0
    spam_score: float = 0.0
    spam_terms: list[str] = field(default_factory=list)
    generic_praise_hits: int = 0

    author_len: int = 0
    author_digit_ratio: float = 0.0
    author_underscores: int = 0
    author_mixed_case: bool = False
    has_channel: bool = False
    like_count: Optional[int] = None
    reply_count: Optional[int] = None

    posting_hour_utc: Optional[int] = None
    minutes_to_previous: Optional[float] = None
    seconds_to_next: Optional[float] = None
    author_duplicate_count: int = 0
    duplicate_ratio: float = 0.0
    max_similarity: float = 0.0


def _entropy(text: str) -> float:
    """Entropia de Shannon de la distribucion de caracteres (0..~4.2)."""
    if not text:
        return 0.0
    counts = Counter(text)
    length = len(text)
    return -sum((c / length) * math.log2(c / length) for c in counts.values())


def build_features(comment: RawComment) -> CommentFeatures:
    """Feature engineering puro (sin IO) sobre un comentario."""
    text = comment.text or ""
    lower = text.lower()
    words = WORD_RE.findall(lower)
    word_count = len(words)
    length = len(text)

    vowels = sum(1 for ch in lower if ch in "aeiou")
    non_latin = sum(1 for ch in text if ord(ch) > 0x2FFF or 0x0400 <= ord(ch) <= 0x04FF)
    digits = sum(1 for ch in text if ch.isdigit())
    uppercase = sum(1 for ch in text if ch.isupper())
    non_space = sum(1 for ch in text if not ch.isspace()) or 1

    # Lexicon de spam: se acumulan los pesos (top 3) y se satura en 1.0.
    hits: list[tuple[str, float]] = []
    for term, weight in SPAM_LEXICON.items():
        if re.search(rf"(?<!\w){re.escape(term)}(?!\w)", lower):
            hits.append((term, weight))
    hits.sort(key=lambda pair: pair[1], reverse=True)
    spam_score = min(1.0, sum(weight for _, weight in hits[:3]))

    author = comment.author or ""
    author_letters = [ch for ch in author if not ch.isdigit()]

    features = CommentFeatures(
        comment_id=comment.comment_id,
        author=author,
        text=text,
        text_preview=text[:220] + ("…" if len(text) > 220 else ""),
        published_at=comment.published_at.isoformat() if comment.published_at else None,
        depth=comment.depth,
        is_author=comment.is_author,
        text_length=length,
        word_count=word_count,
        unique_word_ratio=round(len(set(words)) / word_count, 4) if word_count else 0.0,
        avg_word_length=round(statistics.fmean([len(w) for w in words]), 3) if words else 0.0,
        stopword_ratio=round(sum(1 for w in words if w in STOPWORDS) / word_count, 4) if word_count else 0.0,
        vowel_ratio=round(vowels / non_space, 4),
        char_entropy=round(_entropy(lower), 4),
        emoji_count=len(EMOJI_RE.findall(text)),
        emoji_density=round(len(EMOJI_RE.findall(text)) / length, 4) if length else 0.0,
        link_count=len(URL_RE.findall(text)),
        digit_ratio=round(digits / non_space, 4),
        uppercase_ratio=round(uppercase / non_space, 4),
        allcaps_words=len(ALLCAPS_WORD_RE.findall(text)),
        exclamation_count=text.count("!"),
        repeated_char_groups=len(REPEAT_CHARS_RE.findall(text)),
        question_count=text.count("?"),
        non_latin_ratio=round(non_latin / non_space, 4),
        spam_score=round(spam_score, 4),
        spam_terms=[term for term, _ in hits[:5]],
        generic_praise_hits=len(GENERIC_PRAISE_RE.findall(text)),
        author_len=len(author),
        author_digit_ratio=round(sum(1 for ch in author if ch.isdigit()) / len(author), 4) if author else 0.0,
        author_underscores=author.count("_"),
        author_mixed_case=bool(author_letters) and (
            any(c.isupper() for c in author_letters) and any(c.islower() for c in author_letters)
        ),
        has_channel=comment.has_channel,
        like_count=comment.like_count,
        reply_count=comment.reply_count,
        posting_hour_utc=comment.published_at.hour if comment.published_at else None,
    )
    return features


def enrich_collective_features(all_features: list[CommentFeatures]) -> None:
    """Segunda pasada: features que requieren ver el conjunto de comentarios.

    Aqui se detectan los patrones que solo aparecen en la coleccion:
    rafagas temporales, duplicados exactos y autores que se repiten.
    """
    if not all_features:
        return

    ordered = sorted(all_features, key=lambda f: (f.published_at or ""))

    # --- Repeticiones del mismo autor
    per_author = Counter(f.author for f in all_features)
    for f in all_features:
        f.author_duplicate_count = per_author[f.author] - 1

    # --- Vecindad temporal (solo entre comentarios del mismo nivel)
    top_level = [f for f in ordered if f.depth == 0]
    for previous, current in zip(top_level, top_level[1:]):
        if previous.published_at and current.published_at:
            gap = (datetime.fromisoformat(current.published_at) - datetime.fromisoformat(previous.published_at)).total_seconds()
            current.minutes_to_previous = round(gap / 60, 3)
    for current, following in zip(top_level, top_level[1:]):
        if current.published_at and following.published_at:
            gap = (datetime.fromisoformat(following.published_at) - datetime.fromisoformat(current.published_at)).total_seconds()
            current.seconds_to_next = round(gap, 3)

    # --- Duplicados y similitud (Jaccard de trigramas de palabra)
    signatures = {f.comment_id: _signature(f.text) for f in all_features}
    exact_counter: Counter = Counter(signatures.values())
    for f in all_features:
        signature = signatures[f.comment_id]
        f.duplicate_ratio = round(exact_counter[signature] / len(all_features), 4) if signature else 0.0

    # Coste O(n * m) aceptable para el limite del MVP (n <= 300).
    for i, left in enumerate(all_features):
        left_signature = signatures[left.comment_id]
        if not left_signature:
            continue
        best = 0.0
        for right in all_features[i + 1:]:
            right_signature = signatures[right.comment_id]
            if not right_signature:
                continue
            best = max(best, _jaccard(left_signature, right_signature))
            if best > 0.98:
                break
        left.max_similarity = round(best, 4)


def _signature(text: str) -> frozenset:
    """Firma bag-of-words en minusculas, ignorando el orden."""
    return frozenset(WORD_RE.findall((text or "").lower()))


def _jaccard(left: frozenset, right: frozenset) -> float:
    if not left or not right:
        return 0.0
    intersection = len(left & right)
    if not intersection:
        return 0.0
    return intersection / len(left | right)


# ===========================================================================
# 6. MOTOR HEURISTICO (10 reglas ponderadas)
# ===========================================================================

@dataclass(frozen=True)
class Rule:
    """Una heuristica. `evaluate` devuelve (valor 0..1, evidencia legible)."""

    key: str
    label: str
    weight: float
    description: str
    evaluate: Callable[[CommentFeatures], tuple[float, str]]


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _r_spam(f: CommentFeatures) -> tuple[float, str]:
    """Lexicon de spam + terminosoui, cuenta de enlaces y ctes. promocionales."""
    link_component = _clamp(f.link_count * 0.55, 0, 0.7)
    value = _clamp(0.75 * f.spam_score + link_component)
    evidence = f"terminos spam: {', '.join(f.spam_terms[:4])}" if f.spam_terms else "sin terminos de spam"
    if f.link_count:
        evidence += f" | {f.link_count} enlace(s) externo(s)"
    return value, evidence


def _r_identity(f: CommentFeatures) -> tuple[float, str]:
    """Patrones de nombre de usuario asociados a cuentas farming."""
    value = 0.0
    signals: list[str] = []
    if f.author_digit_ratio > 0.25:
        value += 0.4
        signals.append("muchos digitos en el usuario")
    if f.author_underscores >= 2:
        value += 0.2
        signals.append("guiones bajos multiples")
    if f.author_mixed_case:
        value += 0.15
        signals.append("mayusculas/minusculas mezcladas")
    if re.fullmatch(r"[a-zA-Z]{6,}\d{4,}", f.author or ""):
        value += 0.35
        signals.append("patron letras+numeros aleatorio")
    if re.search(r"(19|20)\d{2}$", f.author or "") and not f.has_channel:
        value += 0.1
        signals.append("terminado en anyo")
    return _clamp(value), (" | ".join(signals) if signals else "nombre de usuario aparentemente normal")


def _r_generic(f: CommentFeatures) -> tuple[float, str]:
    """Elogio generico / texto repetitivo: poca informacion semantica."""
    value = 0.0
    signals: list[str] = []
    if f.generic_praise_hits:
        value += min(0.55, 0.3 * f.generic_praise_hits)
        signals.append("elogio generico")
    # Ratio de palabras unicas bajo = texto repetitivo.
    if f.word_count >= 3 and f.unique_word_ratio < 0.55:
        value += min(0.35, (0.55 - f.unique_word_ratio) * 0.7)
        signals.append("mucha repeticion de palabras")
    # Comentarios muy cortos sin interrogacion ni datos.
    if f.word_count <= 3 and f.question_count == 0 and f.text_length <= 12:
        value += 0.3
        signals.append("muy corto y sin contenido")
    return _clamp(value), (" | ".join(signals) if signals else "contiene contenido especifico")


def _r_noise(f: CommentFeatures) -> tuple[float, str]:
    """Ruido: entropia atipica, deficit de vocales, caracteres repetidos."""
    value = 0.0
    signals: list[str] = []
    if f.char_entropy > 3.9 and f.vowel_ratio < 0.12:
        value += 0.45
        signals.append("cadena casi sin vocales (texto generado)")
    elif f.char_entropy > 4.3:
        value += 0.25
        signals.append("entropia muy alta")
    elif 0 < f.char_entropy < 2.4 and f.text_length > 25:
        value += 0.35
        signals.append("entropia muy baja (texto repetido)")
    if f.repeated_char_groups >= 2:
        value += min(0.35, 0.15 * f.repeated_char_groups)
        signals.append("caracteres repetidos (aaaa / !!!!)")
    if f.non_latin_ratio > 0.08:
        value += min(0.25, f.non_latin_ratio * 2)
        signals.append("mezcla de alfabetos inusual")
    return _clamp(value), (" | ".join(signals) if signals else "texto linguisticamente coherente")


def _r_emojis(f: CommentFeatures) -> tuple[float, str]:
    value = 0.0
    signals: list[str] = []
    if f.emoji_count >= 8:
        value += 0.55
        signals.append(f"{f.emoji_count} emojis (densidad {f.emoji_density:.2%})")
    elif f.emoji_count >= 4:
        value += 0.3
        signals.append(f"{f.emoji_count} emojis")
    if f.emoji_density > 0.15:
        value += 0.35
        signals.append("emojis equivalentes a texto")
    return _clamp(value), (" | ".join(signals) if signals else "uso normal de emojis")


def _r_formatting(f: CommentFeatures) -> tuple[float, str]:
    value = 0.0
    signals: list[str] = []
    if f.uppercase_ratio > 0.6:
        value += 0.45
        signals.append(f"MAYUSCULAS ({f.uppercase_ratio:.0%})")
    if f.allcaps_words >= 2:
        value += min(0.3, 0.15 * f.allcaps_words)
        signals.append(f"{f.allcaps_words} palabras en mayusculas")
    if f.exclamation_count >= 4:
        value += min(0.35, 0.12 * f.exclamation_count)
        signals.append(f"{f.exclamation_count} signos de exclamacion")
    if f.digit_ratio > 0.3:
        value += 0.25
        signals.append("exceso de digitos")
    return _clamp(value), (" | ".join(signals) if signals else "formato normal")


def _r_timing(f: CommentFeatures) -> tuple[float, str]:
    value = 0.0
    signals: list[str] = []
    # Ventana clasica de automatizacion: 02:00-05:00 UTC.
    if f.posting_hour_utc is not None and 2 <= f.posting_hour_utc <= 5:
        value += 0.4
        signals.append(f"publicado a las {f.posting_hour_utc:02d}:00 UTC")
    if f.minutes_to_previous is not None and 0 <= f.minutes_to_previous <= 2:
        value += 0.5
        signals.append(f"llega {f.minutes_to_previous:.1f} min despues del anterior")
    if f.seconds_to_next is not None and 0 <= f.seconds_to_next <= 30:
        value += 0.25
        signals.append(f"otro comentario en {f.seconds_to_next:.0f} s")
    return _clamp(value), (" | ".join(signals) if signals else "sin patrones temporales")


def _r_duplication(f: CommentFeatures) -> tuple[float, str]:
    value = 0.0
    signals: list[str] = []
    if f.duplicate_ratio > 0.02:
        value += _clamp(f.duplicate_ratio * 2.5)
        signals.append(f"texto repetido {f.duplicate_ratio:.1%} del dataset")
    if f.max_similarity > 0.7:
        value += _clamp((f.max_similarity - 0.7) * 2.2)
        signals.append(f"casi duplicado de otro comentario (similitud {f.max_similarity:.0%})")
    if f.author_duplicate_count >= 2:
        value += _clamp(f.author_duplicate_count * 0.18, 0, 0.5)
        signals.append(f"el mismo autor comenta {f.author_duplicate_count + 1} veces")
    return _clamp(value), (" | ".join(signals) if signals else "sin duplicados")


def _r_engagement(f: CommentFeatures) -> tuple[float, str]:
    value = 0.0
    signals: list[str] = []
    if not f.has_channel:
        value += 0.3
        signals.append("cuenta sin canal")
    likes = f.like_count
    if likes is not None and likes == 0 and f.text_length > 40:
        value += 0.3
        signals.append("texto largo con 0 likes")
    if f.reply_count == 0 and f.link_count > 0:
        value += 0.2
        signals.append("enlace sin respuestas de la comunidad")
    if likes is not None and likes >= 3 and f.spam_score > 0.5:
        value -= 0.2   # un spam con likes suele ser contenido organico mal clasificado
        signals.append("pero con likes de la comunidad")
    return _clamp(value), (" | ".join(signals) if signals else "interaccion normal")


RULES: tuple[Rule, ...] = (
    Rule("spam_lexicon", "Lexicon de spam y enlaces", 0.22,
         "Terminos promocionales, enlaces externos y llamadas a la accion.",
         _r_spam),
    Rule("identity_pattern", "Patron de identidad", 0.11,
         "Nombres de usuario tipicos de cuentas creadas en cadena (farming).",
         _r_identity),
    Rule("generic_content", "Contenido generico", 0.11,
         "Elogios genericos, texto muy corto o con alta repeticion de palabras.",
         _r_generic),
    Rule("text_noise", "Ruido textual", 0.10,
         "Entropia atipica, deficit de vocales y caracteres repetidos.",
         _r_noise),
    Rule("temporal", "Patron temporal", 0.10,
         "Franja horaria de automatizacion y rafagas de publicacion.",
         _r_timing),
    Rule("formatting", "Formato atipico", 0.10,
         "MAYUSCULAS, exceso de exclamaciones o de digitos.",
         _r_formatting),
    Rule("duplication", "Duplicacion", 0.08,
         "Comentarios repetidos, casi duplicados o autores que comentan en bucle.",
         _r_duplication),
    Rule("emojis", "Abuso de emojis", 0.08,
         "Densidad de emojis muy superior a la de un comentario natural.",
         _r_emojis),
    Rule("engagement", "Desajuste de interaccion", 0.10,
         "Cuentas sin canal, cero likes en texto largo o enlaces sin respuesta.",
         _r_engagement),
)

assert math.isclose(sum(rule.weight for rule in RULES), 1.0), "Los pesos deben sumar 1.0"

# Curva de calibracion: score crudo -> probabilidad. Mapea [0.08, 0.80] a [0, 1].
RAW_FLOOR = 0.08
RAW_CEIL = 0.80


def squash(raw_score: float) -> float:
    """Normaliza el score ponderado crudo a una probabilidad 0..1."""
    return _clamp((raw_score - RAW_FLOOR) / (RAW_CEIL - RAW_FLOOR))


@dataclass
class Verdict:
    features: CommentFeatures
    bot_score: float                 # 0..100 probabilidad de bot
    raw_score: float                 # suma ponderada 0..1
    reasons: list[str]
    contributions: dict[str, float]  # regla -> contribucion en puntos (0..100)


def score_comment(features: CommentFeatures) -> Verdict:
    """Aplica las 10 heuristicas y devuelve el veredicto con su evidencia."""
    contributions: dict[str, float] = {}
    reasons: list[str] = []
    raw = 0.0

    for rule in RULES:
        value, evidence = rule.evaluate(features)
        weighted = rule.weight * value
        raw += weighted
        contributions[rule.key] = round(weighted * 100, 2)
        # Solo se reportan las reglas que aportan al menos 8 puntos.
        if weighted >= 0.08:
            reasons.append(f"{rule.label}: {evidence}")

    if not reasons:
        reasons.append("Ninguna heuristica supera el umbral: patron indistinguible de un humano.")

    return Verdict(
        features=features,
        bot_score=round(squash(raw) * 100, 1),
        raw_score=round(raw, 4),
        reasons=reasons[:4],
        contributions=contributions,
    )


# ===========================================================================
# 7. AGREGACION (pandas)
# ===========================================================================

HIGH_THRESHOLD = 60.0
MEDIUM_THRESHOLD = 30.0
# Confianza baja en modo simulado: es una demo, no una medida real.
SIMULATED_CONFIDENCE_CAP = 55.0


def aggregate(verdicts: list[Verdict], source: str) -> tuple[dict[str, Any], list[str], float]:
    """Consolida los veredictos en KPIs, distribucion y senales dominantes."""
    if not verdicts:
        raise HTTPException(status_code=422, detail="No hay comentarios suficientes para analizar.")

    frame = pd.DataFrame(
        [
            {
                "bot_score": v.bot_score,
                "raw_score": v.raw_score,
                **{f"rule_{key}": value for key, value in v.contributions.items()},
                "link_count": v.features.link_count,
                "emoji_count": v.features.emoji_count,
                "text_length": v.features.text_length,
                "spam_score": v.features.spam_score,
                "duplicate_ratio": v.features.duplicate_ratio,
                "depth": v.features.depth,
                "author": v.features.author,
            }
            for v in verdicts
        ]
    )

    scores = frame["bot_score"]
    base_percentage = float(scores.mean())

    # --- Efecto cluster: los bots no llegan solos, llegan en grupo.
    duplicate_ratio = float(frame["duplicate_ratio"].max())
    burst_detected = bool(
        (frame["rule_temporal"] >= 25).any()
    )
    amplifier = 1.0
    amplifier_reasons: list[str] = []
    if duplicate_ratio >= 0.05:
        factor = _clamp((duplicate_ratio - 0.05) * 1.4, 0, 0.12)
        amplifier += factor
        amplifier_reasons.append(f"duplicacion detectada ({duplicate_ratio:.0%} de un mismo texto)")
    if burst_detected:
        amplifier += 0.05
        amplifier_reasons.append("rafagas de publicacion en ventana de automatizacion")
    if frame["rule_spam_lexicon"].mean() >= 35:
        amplifier += 0.04
        amplifier_reasons.append("presencia de campanas de spam en el conjunto")

    bot_percentage = round(_clamp(base_percentage * amplifier, 0, 100), 1)

    # --- Distribucion
    distribution = {
        "high": int((scores >= HIGH_THRESHOLD).sum()),
        "medium": int(((scores >= MEDIUM_THRESHOLD) & (scores < HIGH_THRESHOLD)).sum()),
        "low": int((scores < MEDIUM_THRESHOLD).sum()),
    }

    # --- Senales dominantes (media de contribucion por regla)
    rule_columns = [f"rule_{rule.key}" for rule in RULES]
    signal_means = frame[rule_columns].mean().sort_values(ascending=False)
    signals = [
        {
            "key": column.removeprefix("rule_"),
            "label": next(r.label for r in RULES if r.key == column.removeprefix("rule_")),
            "description": next(r.description for r in RULES if r.key == column.removeprefix("rule_")),
            "weight": round(next(r.weight for r in RULES if r.key == column.removeprefix("rule_")), 3),
            "average_contribution": round(float(value), 2),
            "flagged": int((frame[column] > 0).sum()),
        }
        for column, value in signal_means.items()
        if float(value) > 0.01
    ]

    # --- Confianza: grows con el tamano de muestra y con la nitidez del reparto.
    sample_factor = _clamp(len(verdicts) / 100)
    separation = _clamp(statistics.pstdev(scores.tolist(), mu=base_percentage) / 30)
    agreement = _clamp(1 - (bot_percentage / 100) * 0.4) if bot_percentage <= 50 else _clamp(1 - ((100 - bot_percentage) / 100) * 0.4)
    confidence = round((0.45 * sample_factor + 0.30 * separation + 0.25 * agreement) * 100, 1)
    if source == "simulated":
        confidence = round(min(confidence, SIMULATED_CONFIDENCE_CAP), 1)

    # --- Medias de features clave para interpretar el resultado
    # Todos los valores son numericos: el frontend los formatea directamente.
    feature_averages = {
        "longitud_media_caracteres": round(float(frame["text_length"].mean()), 1),
        "media_enlaces_por_comentario": round(float(frame["link_count"].mean()), 2),
        "media_emojis_por_comentario": round(float(frame["emoji_count"].mean()), 2),
        "score_bruto_medio": round(float(frame["raw_score"].mean()) * 100, 1),
        "porcentaje_comentarios_con_enlace": round(float((frame["link_count"] > 0).mean()) * 100, 1),
        "autores_unicos": int(frame["author"].nunique()),
    }

    gaps = [
        v.features.seconds_to_next for v in verdicts
        if v.features.seconds_to_next is not None
    ]
    metrics: dict[str, Any] = {
        "total_comments_analyzed": len(verdicts),
        "unique_authors": int(frame["author"].nunique()),
        "replies_included": int((frame["depth"] == 1).sum()),
        "bot_percentage": bot_percentage,
        "human_percentage": round(100 - bot_percentage, 1),
        "suspected_bot_comments": distribution["high"] + distribution["medium"],
        "mean_bot_score": round(base_percentage, 1),
        "median_bot_score": round(float(scores.median()), 1),
        "max_bot_score": round(float(scores.max()), 1),
        "min_bot_score": round(float(scores.min()), 1),
        "score_std_dev": round(float(scores.std(ddof=0)), 1),
        "comments_with_links": int((frame["link_count"] > 0).sum()),
        "link_ratio": round(float((frame["link_count"] > 0).mean()) * 100, 1),
        "emoji_per_comment": round(float(frame["emoji_count"].mean()), 2),
        "duplicate_ratio": round(duplicate_ratio * 100, 1),
        "burst_detected": burst_detected,
        "median_gap_seconds": round(statistics.median(gaps), 1) if gaps else None,
        "confidence": confidence,
        "cluster_amplifier": round(amplifier, 3),
        "cluster_reasons": amplifier_reasons,
    }
    return metrics, signals, feature_averages


def build_findings(metrics: dict[str, Any], signals: list[dict[str, Any]], source: str) -> list[str]:
    """Conclusiones en lenguaje natural a partir de las metricas."""
    findings: list[str] = []

    risk = metrics["risk_level"]
    findings.append(
        f"Nivel de riesgo {risk.upper()}: {metrics['bot_percentage']}% de los comentarios analizados "
        f"presentan patrones de automatizacion (confianza {metrics['confidence']}%)."
    )

    if signals:
        top = signals[0]
        findings.append(
            f"La senal dominante es «{top['label']}» ({top['average_contribution']} puntos de media), "
            f"presente en {top['flagged']} de {metrics['total_comments_analyzed']} comentarios."
        )

    if metrics["link_ratio"] >= 15:
        findings.append(
            f"El {metrics['link_ratio']}% de los comentarios contiene enlaces externos: "
            "revisar manualmente por spam o phishing."
        )

    if metrics["duplicate_ratio"] >= 5:
        findings.append(
            f"Hay un {metrics['duplicate_ratio']}% de texto duplicado, indicativo de una "
            "campaña coordinada o de un honeypot de spam."
        )

    if metrics["burst_detected"]:
        findings.append("Se detectaron rafagas de publicacion, tipicas de cuentas automatizadas.")

    if metrics["score_std_dev"] < 12:
        findings.append(
            "La puntuacion de todos los comentarios es muy parecida: esto puede indicar que el "
            "analizador no discrimina bien en este video, no que el canal este 100% automatizado."
        )

    if source == "simulated":
        findings.append(
            "ATENCION: los datos son SIMULADOS (falta YOUTUBE_API_KEY). "
            "Las cifras sirven para validar la interfaz, no para tomar decisiones."
        )

    return findings


def risk_profile(bot_percentage: float, source: str) -> dict[str, Any]:
    """Traduce el porcentaje a un nivel de riesgo accionable."""
    if bot_percentage >= 60:
        level, label = "critical", "Critico"
        action = "Revisar y limpiar la seccion de comentarios; suspicion de campana de spam coordinada."
    elif bot_percentage >= 40:
        level, label = "high", "Alto"
        action = "Auditar manualmente los comentarios marcados antes de interactuar con la comunidad."
    elif bot_percentage >= 25:
        level, label = "medium", "Medio"
        action = "Vigilar: hay presencia noticeable de automatizacion, todavia compatible con usuarios reales."
    else:
        level, label = "low", "Bajo"
        action = "Buena señal: la conversacion parece predominantemente organica."

    if source == "simulated":
        action += " (Estimacion sobre datos simulados, no concluyente.)"

    return {"level": level, "label": label, "action": action}


# ===========================================================================
# 8. ORQUESTACION
# ===========================================================================

def run_analysis(video_id: str, limit: int, include_comments: bool = True) -> AnalyzeResponse:
    """Pipeline completo: extraccion -> features -> scoring -> agregacion."""
    dataset = fetch_youtube_comments(video_id, limit)
    if not dataset.comments:
        raise HTTPException(
            status_code=422,
            detail="No se obtuvo ningun comentario para analizar. Prueba con otro video.",
        )

    features = [build_features(comment) for comment in dataset.comments]
    enrich_collective_features(features)
    verdicts = [score_comment(f) for f in features]

    metrics, signals, feature_averages = aggregate(verdicts, dataset.source)
    profile = risk_profile(metrics["bot_percentage"], dataset.source)
    metrics["risk_level"] = profile["level"]

    findings = build_findings(metrics, signals, dataset.source)

    ordered = sorted(verdicts, key=lambda v: v.bot_score, reverse=True)
    comment_payload = [
        {
            "comment_id": v.features.comment_id,
            "author": v.features.author,
            "text_preview": v.features.text_preview,
            "published_at": v.features.published_at,
            "bot_score": v.bot_score,
            "verdict": verdict_label(v.bot_score),
            "is_reply": v.features.depth == 1,
            "reasons": v.reasons,
            "features": {
                "text_length": v.features.text_length,
                "word_count": v.features.word_count,
                "unique_word_ratio": v.features.unique_word_ratio,
                "emoji_count": v.features.emoji_count,
                "link_count": v.features.link_count,
                "spam_score": v.features.spam_score,
                "spam_terms": v.features.spam_terms,
                "uppercase_ratio": v.features.uppercase_ratio,
                "char_entropy": v.features.char_entropy,
                "posting_hour_utc": v.features.posting_hour_utc,
                "author_duplicate_count": v.features.author_duplicate_count,
                "duplicate_ratio": v.features.duplicate_ratio,
                "max_similarity": v.features.max_similarity,
                "like_count": v.features.like_count,
                "reply_count": v.features.reply_count,
                "has_channel": v.features.has_channel,
            },
        }
        for v in (ordered if include_comments else [])
    ]

    return AnalyzeResponse(
        analysis_id=uuid.uuid4().hex[:12],
        generated_at=datetime.now(timezone.utc).isoformat(),
        app_version=settings.app_version,
        dataset_source=dataset.source,
        source={
            "video_id": video_id,
            "url": canonical_url(video_id),
            **dataset.video_meta,
        },
        metrics=metrics,
        risk=profile,
        distribution={
            "high": sum(1 for v in verdicts if v.bot_score >= HIGH_THRESHOLD),
            "medium": sum(1 for v in verdicts if MEDIUM_THRESHOLD <= v.bot_score < HIGH_THRESHOLD),
            "low": sum(1 for v in verdicts if v.bot_score < MEDIUM_THRESHOLD),
        },
        signals=signals,
        feature_averages=feature_averages,
        findings=findings,
        comments=comment_payload,
        warnings=dataset.warnings,
    )


def verdict_label(score: float) -> str:
    if score >= HIGH_THRESHOLD:
        return "high"
    if score >= MEDIUM_THRESHOLD:
        return "medium"
    return "low"


# ===========================================================================
# 9. FASTAPI
# ===========================================================================

class Utf8JSONResponse(JSONResponse):
    """JSON con `charset=utf-8` explicito.

    RFC 8259 dice que JSON siempre es UTF-8, pero clientes legacy (PowerShell
    5.1, curl.exe en consolas con codepage legacy) asumen latin-1 cuando el
    Content-Type no lo declara, y destrozan los acentos y los emojis.
    """

    media_type = "application/json; charset=utf-8"


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Analisis heuristico de comentarios para estimar la presencia de bots.\n\n"
        "**Pipeline**: extraccion (YouTube Data API v3 o simulador determinista) -> "
        "ingenieria de features -> 9 heuristicas ponderadas -> agregacion con pandas.\n\n"
        "> El resultado es una estimacion orientativa, no una verdad absoluta."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
    default_response_class=Utf8JSONResponse,
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.exception_handler(ValueError)
async def value_error_handler(_: Request, exc: ValueError) -> Utf8JSONResponse:
    return Utf8JSONResponse(status_code=400, content={"detail": str(exc)})


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index(request: Request) -> HTMLResponse:
    """Interfaz web minima (Tailwind por CDN, sin build step)."""
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "app_name": settings.app_name,
            "app_version": settings.app_version,
            "data_mode": "real" if settings.youtube_api_key else "simulated",
            "default_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["sistema"])
async def health() -> HealthResponse:
    """Liveness probe usado por Docker HEALTHCHECK."""
    return HealthResponse(
        status="ok",
        version=settings.app_version,
        environment=settings.environment,
        data_mode="youtube_api" if settings.youtube_api_key else "simulated",
    )


@app.get("/api/config", tags=["sistema"])
async def api_config() -> dict[str, Any]:
    """Metadatos del servicio para que el frontend se adapte."""
    return {
        "app_name": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment,
        "data_mode": "youtube_api" if settings.youtube_api_key else "simulated",
        "max_comments": settings.max_comments,
        "supported_platforms": ["youtube"],
        "scoring": {
            "heuristics": [{"key": r.key, "label": r.label, "weight": r.weight} for r in RULES],
            "high_threshold": HIGH_THRESHOLD,
            "medium_threshold": MEDIUM_THRESHOLD,
        },
    }


@app.post("/api/analyze", response_model=AnalyzeResponse, tags=["analisis"])
async def analyze(payload: AnalyzeRequest) -> AnalyzeResponse:
    """Analiza los comentarios de un video y devuelve la puntuacion de bots.

    Cuerpo: JSON con `url` (obligatorio), `comment_limit` e `include_comments`
    (opcionales). Ejemplo con curl:

        curl -X POST http://localhost:8000/api/analyze ^
             -H "Content-Type: application/json" ^
             -d "{\\"url\\":\\"https://youtu.be/dQw4w9WgXcQ\\",\\"comment_limit\\":60}"
    """
    video_id = resolve_video(payload.url)
    limit = payload.comment_limit or settings.max_comments
    return run_analysis(video_id, limit, include_comments=payload.include_comments)


# Ejecuta con: uvicorn app:app --reload  (desarrollo)  |  docker compose up
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=settings.environment == "development")