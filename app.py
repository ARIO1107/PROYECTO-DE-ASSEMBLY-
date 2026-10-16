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

    1. EXTRACCION   -> `resolve_video()` + `fetch_youtube_comments()`
                       o `parse_uploaded_comments()` (CSV/JSON multiplataforma)
    2. INGENIERIA    -> `build_features()` (features crudas por comentario)
    3. PUNTUACION    -> `RULES` (9 heuristicas ponderadas, 0..1 por regla)
    4. AGREGACION   -> `aggregate()` (pandas) -> respuesta JSON

Las tres fuentes de datos (YouTube, archivo subido y simulacion) terminan en el
mismo `RawComment`, asi que comparten entero de la fase 2 a la 4: no existe una
ruta alternativa de puntuacion para archivos, y cualquier cambio del motor vale
para las tres.

Ademas hay un modo de demostracion (`/api/demo/*`) que genera un ataque de bots
en memoria y lo emite en vivo por Server-Sent Events, sin tocar la red ni la
cuota. Reutiliza EXACTAMENTE el mismo motor: las demo no tienen logica de scoring
propia, solo una forma distinta de entregar los resultados.

Diseno del scoring (importante)
-------------------------------
Cada comentario obtiene un score 0..100 = suma ponderada de las heuristicas.
El score NO se usa como veredicto absoluto: es una senal heuristica orientativa
que debe interpretarse junto con las metricas de contexto del canal.
El agregado aplica ademas un "efecto cluster" porque los bots suelen llegar en
ráfagas y de forma duplicada, no de manera independiente.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import math
import os
import random
import re
import statistics
import time
import unicodedata
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Sequence
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
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

    # --- Parametros de la YouTube Data API v3 ------------------------------
    # Cada llamada a commentThreads.list cuesta 1 unidad de las 10.000/dia
    # gratuitas, con independencia de cuantos comentarios devuelva.
    max_api_pages: int = Field(default=5, ge=1, le=20, validation_alias=AliasChoices("MAX_API_PAGES"))
    api_quota_per_page: int = Field(default=1, ge=1, le=100, validation_alias=AliasChoices("API_QUOTA_PER_PAGE"))
    api_retries: int = Field(default=3, ge=1, le=6, validation_alias=AliasChoices("API_RETRIES"))
    api_backoff_seconds: float = Field(default=1.0, ge=0.1, le=30, validation_alias=AliasChoices("API_BACKOFF_SECONDS"))


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
    dataset_source: str                      # "youtube_api" | "simulated" | "file_upload"
    source: dict[str, Any]                    # video_id, url canonica, titulo, canal
    api: dict[str, Any] = Field(default_factory=dict)  # traza de la llamada real
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
    author_channel_id: Optional[str] = None
    is_author: bool = False             # es el creador del video
    depth: int = 0                     # 0 = principal, 1 = respuesta


@dataclass
class Dataset:
    comments: list[RawComment]
    source: str                        # "youtube_api" | "simulated" | "file_upload"
    warnings: list[str] = field(default_factory=list)
    video_meta: dict[str, Any] = field(default_factory=dict)
    api_info: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Errores de la API: se clasifican para poder explicar QUE fallo, en vez de
# devolver un HTTP generico que el usuario no puede interpretar.
# ---------------------------------------------------------------------------

# Traduccion de los `reason` que devuelve la API a un mensaje accionable.
API_ERROR_HINTS: dict[str, str] = {
    "quotaExceeded": "cuota diaria agotada (10.000 unidades/dia). Se reinicia a medianoche (hora del Pacifico).",
    "dailyLimitExceeded": "cuota diaria agotada. Se reinicia a medianoche (hora del Pacifico).",
    "rateLimitExceeded": "demasiadas peticiones por minuto: espera unos segundos.",
    "userRateLimitExceeded": "demasiadas peticiones por minuto: espera unos segundos.",
    "commentsDisabled": "el creador tiene los comentarios desactivados en este video.",
    "commentsNotEnabled": "los comentarios no estan habilitados en este video.",
    "commentsAreDisabled": "los comentarios estan desactivados en este video.",
    "keyInvalid": "YOUTUBE_API_KEY no es valida: revisa que no tenga comillas ni espacios.",
    "keyNotFound": "YOUTUBE_API_KEY no es valida o ha sido revocada.",
    # YouTube responde `badRequest` (no `keyInvalid`) cuando la clave no existe
    # o no tiene el formato AIza..., asi que se detecta tambien por mensaje.
    "badRequest": "peticion rechazada por la API: revisa la clave y que la YouTube Data API v3 este habilitada.",
    "forbidden": "la clave no tiene permiso sobre la YouTube Data API v3 (habilitala en Google Cloud Console).",
    "accessNotConfigured": "la YouTube Data API v3 no esta habilitada en tu proyecto de Google Cloud.",
    "videoNotFound": "el video no existe, es privado o fue eliminado.",
    "resourceNotFound": "el video no existe o no es accesible con esta clave.",
    "invalidParameter": "parametros no validos: revisa que el ID del video sea correcto.",
    # Devuelto por la API real al pedir una `part` inexistente.
    "unknownPart": "la API no reconoce alguna de las partes pedidas.",
    "serviceUnavailable": "servicio de YouTube temporalmente no disponible.",
    "backendError": "error interno de YouTube.",
    "networkError": "no se pudo contactar con googleapis.com (red, DNS o proxy).",
}


class YouTubeApiError(Exception):
    """Error normalizado de la YouTube Data API v3."""

    def __init__(self, status_code: int, reason: str, message: str) -> None:
        super().__init__(f"[{status_code}/{reason}] {message}")
        self.status_code = status_code
        self.reason = reason
        self.message = message

    @property
    def is_quota_exhausted(self) -> bool:
        """La cuota se ha agotado: no adianta reintentar, hay que esperar al reinicio."""
        return self.reason in {"quotaExceeded", "dailyLimitExceeded"}

    @property
    def comments_are_disabled(self) -> bool:
        return self.reason in {"commentsDisabled", "commentsNotEnabled", "commentsAreDisabled"}

    @property
    def is_retryable(self) -> bool:
        """429 (rate limit) y 5xx se pueden reintentar con backoff."""
        return self.status_code == 429 or self.status_code >= 500 or self.reason in {
            "serviceUnavailable", "backendError", "networkError",
        }

    def human_readable(self) -> str:
        """Mensaje para el usuario final, en castellano y sin jerga interna."""
        # La API usa `badRequest` tanto para una clave invalida como para otros
        # problemas de peticion: se desambigua por el texto del mensaje.
        message_lower = self.message.lower()
        if "api key not valid" in message_lower or "api key not authorized" in message_lower:
            return (
                f"HTTP {self.status_code} ({self.reason}): la clave de la API no es valida. "
                "Comprueba YOUTUBE_API_KEY en el entorno y que la YouTube Data API v3 "
                "esté habilitada en tu proyecto de Google Cloud."
            )

        hint = API_ERROR_HINTS.get(self.reason)
        base = f"HTTP {self.status_code} ({self.reason})"
        if hint:
            return f"{base}: {hint}"
        return f"{base}: {self.message[:160]}"


def _http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": f"{settings.app_name}/{settings.app_version}"})
    return session


def _parse_json(response: requests.Response) -> dict[str, Any]:
    """Convierte el cuerpo a dict o lanza YouTubeApiError si no es JSON valido."""
    try:
        payload = response.json()
    except ValueError as exc:
        raise YouTubeApiError(
            response.status_code, "invalidJson",
            f"la API no devolvio JSON (HTTP {response.status_code})",
        ) from exc
    if not isinstance(payload, dict):
        raise YouTubeApiError(response.status_code, "invalidPayload", "se esperaba un objeto JSON.")
    return payload


def _extract_error(response: requests.Response) -> tuple[str, str]:
    """Saca (reason, message) del formato de error de Google."""
    try:
        payload = response.json()
    except ValueError:
        return "unknown", response.text[:200]
    if not isinstance(payload, dict):
        return "unknown", response.text[:200]
    error = payload.get("error", {}) or {}
    errors = error.get("errors", []) or []
    reason = errors[0].get("reason", "unknown") if errors else "unknown"
    message = error.get("message") or response.text[:200]
    return reason, str(message)


def _api_get(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """GET a la API con reintentos y backoff exponencial ante 429 / 5xx / red.

    Devuelve el dict JSON o lanza YouTubeApiError. Los errores de cuota
    (quotaExceeded) NO son reintentables: reintentar solo gasta tiempo.
    """
    last_error: Optional[YouTubeApiError] = None

    for attempt in range(1, settings.api_retries + 1):
        try:
            response = session.get(url, params=params, timeout=settings.request_timeout)
        except requests.RequestException as exc:
            # Timeout, error de DNS, TLS o proxy: merece un reintento.
            last_error = YouTubeApiError(0, "networkError", str(exc))
            if attempt >= settings.api_retries:
                raise last_error
            time.sleep(settings.api_backoff_seconds * (2 ** (attempt - 1)))
            continue

        if response.status_code == 200:
            return _parse_json(response)

        reason, message = _extract_error(response)
        last_error = YouTubeApiError(response.status_code, reason, message)

        if not last_error.is_retryable or attempt >= settings.api_retries:
            raise last_error

        # Respeta la cabecera Retry-After si la API la envia (segundos).
        delay = settings.api_backoff_seconds * (2 ** (attempt - 1))
        try:
            retry_after = float(response.headers.get("Retry-After", ""))
            if retry_after > 0:
                delay = min(retry_after, 30.0)
        except (TypeError, ValueError):
            pass
        time.sleep(delay)

    raise last_error or YouTubeApiError(0, "unknown", "fallo desconocido")


# `reason` de la API que SI pueden deberse a la parte `replies` del recurso.
# `unknownPart` es el que devuelve la API real (verificado contra
# googleapis.com con part=snippet,replies(snippet)).
# Importante: NO incluir reasons de autenticacion (keyInvalid, badRequest,
# accessNotConfigured, forbidden) ni de cuota, porque reintentar quitando
# `replies` no arregla nada y solo gasta una unidad de cuota.
REPLIES_FALLBACK_REASONS = frozenset({"invalidParameter", "invalidPart", "unknownPart"})


@dataclass
class FetchStats:
    """Traza de la llamada real, para poder informar de cobertura y cuota."""

    pages: int = 0
    calls: int = 0
    top_level: int = 0
    replies: int = 0
    total_available: Optional[int] = None
    replies_requested: bool = True
    quota_units: int = 0


def _safe_int(value: Any, default: int = 0) -> int:
    """La API serializa los int64 como string: convierte sin romper el flujo."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _fetch_comment_threads(
    session: requests.Session,
    api_key: str,
    video_id: str,
    limit: int,
    owner_channel_id: Optional[str],
    warnings: list[str],
) -> tuple[list[RawComment], FetchStats]:
    """Pagina commentThreads.list hasta cubrir `limit` comentarios.

    Degradaciones controladas:
      * Si la variante con `replies` es rechazada con 400, se reintenta la
        pagina sin `replies` (algunos videos/proyectos lo rechaza).
      * Si no hay `nextPageToken`, se para aunque queden comentarios por pedir.
    """
    comments: list[RawComment] = []
    stats = FetchStats()
    page_token: Optional[str] = None

    while len(comments) < limit and stats.pages < settings.max_api_pages:
        remaining = limit - len(comments)
        params: dict[str, Any] = {
            # Sintaxis verificada contra la API real: `replies` a secas, NO
            # `replies(snippet)`. Con la variante con parentesis la API
            # responde HTTP 400 unknownPart. El bloque `replies` ya trae el
            # `snippet` completo de cada respuesta.
            "part": "snippet,replies" if stats.replies_requested else "snippet",
            "videoId": video_id,
            # La API admite 1..100. Pedir mas de lo necesario gasta cuota igual.
            "maxResults": max(1, min(100, remaining)),
            "order": "relevance",
            "textFormat": "plainText",
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            payload = _api_get(session, YOUTUBE_API_URL, params)
        except YouTubeApiError as exc:
            # Degradacion 1: este video/proyecto no acepta la parte `replies`.
            # Se reintenta la MISMA pagina como `part=snippet` (una sola vez).
            if (
                stats.replies_requested
                and exc.status_code == 400
                and exc.reason in REPLIES_FALLBACK_REASONS
            ):
                warnings.append(
                    "YouTube no devolvio las respuestas de los comentarios: "
                    "se analizan solo los comentarios principales."
                )
                stats.replies_requested = False
                continue
            raise

        stats.pages += 1
        stats.calls += 1
        stats.quota_units += settings.api_quota_per_page

        # `pageInfo.totalResults` de commentThreads NO es el total de
        # comentarios del video (con order=relevance devuelve como mucho la
        # pagina pedida). El total fiable viene de videos.list, asi que aqui
        # solo se guarda como respaldo por si faltara ese dato.
        reported = _safe_int(payload.get("pageInfo", {}).get("totalResults"))
        if reported and (stats.total_available is None or reported > stats.total_available):
            stats.total_available = reported

        for thread in payload.get("items", []):
            snippet = (thread.get("snippet") or {}).get("topLevelComment", {}).get("snippet", {})
            if not snippet:
                continue
            comment = _parse_api_comment(snippet, depth=0)
            comments.append(comment)
            stats.top_level += 1

            for reply in (thread.get("replies") or {}).get("comments", []) or []:
                comments.append(_parse_api_comment(reply.get("snippet", {}), depth=1))
                stats.replies += 1

        page_token = payload.get("nextPageToken")
        if not page_token:
            break

    # El propietario del video no siempre trae `authorIsChannelOwner`: se
    # resuelve tambien comparando su authorChannelId con el del video.
    if owner_channel_id:
        for comment in comments:
            if comment.author_channel_id == owner_channel_id:
                comment.is_author = True

    return comments[:limit], stats


def fetch_youtube_comments(video_id: str, limit: int) -> Dataset:
    """Descarga comentarios REALES con la YouTube Data API v3 (commentThreads.list).

    Flujo:
      1. Sin clave            -> modo simulacion determinista.
      2. Metadatos del video  -> titulo, canal y commentCount (1 unidad de cuota).
      3. Hilos de comentarios -> paginados hasta `limit`.
      4. Cualquier fallo esperable (cuota, comentarios desactivados, video
         inexistente, red caida) cae a simulacion SIN romper la app, y el
         motivo exacto se comunica en `warnings`.
    """
    api_key = (settings.youtube_api_key or "").strip()
    if not api_key:
        return _simulated_dataset(
            video_id, limit,
            "YOUTUBE_API_KEY no configurada: se sirvio un dataset SIMULADO para la demo. "
            "Define la variable (ver .env.example) para analizar comentarios reales.",
        )

    session = _http_session()
    warnings: list[str] = []

    # Los metadatos van primero: dan el canal del video, necesario para detectar
    # al propietario, y no dependen de que haya comentarios.
    meta = _fetch_video_meta(session, api_key, video_id, warnings)

    try:
        comments, stats = _fetch_comment_threads(
            session, api_key, video_id, limit, meta.get("channel_id"), warnings,
        )
    except YouTubeApiError as exc:
        message = (
            f"La API real no devolvio comentarios: {exc.human_readable()}. "
            "Se genero un dataset SIMULADO."
        )
        if exc.is_quota_exhausted:
            message += " Prueba manana o usa una segunda clave."
        return _simulated_dataset(video_id, limit, message, meta)

    if not comments:
        # Si la API responde 200 pero sin items, lo normal es que no haya
        # comentarios publicos o que esten restringidos a respondents.
        return _simulated_dataset(
            video_id, limit,
            "YouTube respondio correctamente pero devolvio 0 comentarios "
            "(pueden estar restringidos a los que respondieron o no existir): "
            "se genero un dataset SIMULADO.",
            meta,
        )

    # El total fiable de comentarios del video es `commentCount` de videos.list,
    # no `pageInfo.totalResults` de commentThreads (que con order=relevance
    # devuelve como mucho el tamano de la pagina). Si falta, se usa el
    # respaldo capturado al paginar.
    available = _safe_int(meta.get("comment_count")) or stats.total_available
    if available and available > len(comments):
        warnings.append(
            f"Se analizaron {len(comments):,} de {available:,} comentarios disponibles "
            f"({stats.pages} {'pagina' if stats.pages == 1 else 'paginas'}, "
            f"~{stats.quota_units} {'unidad' if stats.quota_units == 1 else 'unidades'} "
            "de cuota). Los comentarios de menor relevancia quedan fuera."
        )

    coverage = (
        # round(..., 2) porque en un video muy popular puede ser 0.005 %.
        round(len(comments) / available * 100, 2) if available else None
    )

    return Dataset(
        comments=comments,
        source="youtube_api",
        warnings=warnings,
        video_meta=meta,
        api_info={
            "endpoint": YOUTUBE_API_URL,
            "pages_fetched": stats.pages,
            "api_calls": stats.calls,
            "quota_units": stats.quota_units,
            "top_level_comments": stats.top_level,
            "replies_included": stats.replies,
            "replies_part_available": stats.replies_requested,
            "total_comments_available": available,
            "coverage_ratio": coverage,
            "order": "relevance",
        },
    )


def _simulated_dataset(
    video_id: str,
    limit: int,
    warning: str,
    video_meta: Optional[dict[str, Any]] = None,
) -> Dataset:
    """Dataset simulado + aviso. Conserva los metadatos reales si los hubo."""
    dataset = simulate_comments(video_id, limit)
    dataset.warnings.append(warning)
    if video_meta:
        # Se mezclan: el titulo/canal reales son mejores que los inventados.
        dataset.video_meta = {**dataset.video_meta, **{k: v for k, v in video_meta.items() if v}}
        dataset.video_meta.pop("comment_count", None)
        dataset.video_meta["comment_count"] = len(dataset.comments)
    return dataset


def _parse_api_comment(snippet: dict[str, Any], depth: int) -> RawComment:
    """Traduce el `snippet` de commentThreads.list a nuestro modelo interno.

    Campos usados de la API:
        id, authorDisplayName, authorChannelId, textOriginal, textDisplay,
        publishedAt, likeCount, replyCount, totalReplyCount, authorIsChannelOwner.
    """
    published: Optional[datetime] = None
    raw_date = snippet.get("publishedAt")
    if raw_date:
        try:
            # La API devuelve ISO-8601 con 'Z' final.
            published = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
        except ValueError:
            published = None

    # textOriginal es el texto plano original; textDisplay es la version
    # renderizada. Con textFormat=plainText no hay HTML que limpiar.
    text = snippet.get("textOriginal") or snippet.get("textDisplay") or ""

    # `authorChannelId` NO es un string: llega como {"value": "UC..."}.
    # Aplanarlo aqui evita arrastrar dicts por todo el pipeline y que la
    # comparacion con el canal del video falle silenciosamente.
    channel_id = _channel_id_of(snippet)

    return RawComment(
        comment_id=snippet.get("id") or uuid.uuid4().hex[:12],
        author=snippet.get("authorDisplayName") or "@desconocido",
        text=text,
        published_at=published,
        like_count=_safe_int(snippet.get("likeCount")),
        reply_count=_safe_int(snippet.get("replyCount") or snippet.get("totalReplyCount")),
        has_channel=bool(channel_id),
        author_channel_id=channel_id,
        is_author=bool(snippet.get("authorIsChannelOwner")),
        depth=depth,
    )


def _channel_id_of(snippet: dict[str, Any]) -> Optional[str]:
    """Extrae el ID de canal de un snippet de comentario de la API.

    Segun el endpoint, `authorChannelId` llega como objeto `{"value": "UC..."}`
    (commentThreads) y `videoChannelId` como string plano (videos). Se
    aceptan ambas formas y cualquier otra se descarta.
    """
    raw = snippet.get("authorChannelId") or snippet.get("videoChannelId")
    if isinstance(raw, dict):
        raw = raw.get("value")
    if isinstance(raw, str) and raw:
        return raw
    return None


def _fetch_video_meta(
    session: requests.Session,
    api_key: str,
    video_id: str,
    warnings: list[str],
) -> dict[str, Any]:
    """Metadatos del video (videos.list). Degradado: nunca rompe el analisis."""
    try:
        payload = _api_get(
            session,
            YOUTUBE_VIDEOS_URL,
            {"part": "snippet,statistics", "id": video_id, "key": api_key},
        )
    except YouTubeApiError as exc:
        warnings.append(
            f"No se pudieron obtener los metadatos del video ({exc.reason}); "
            "el analisis de comentarios no se ve afectado."
        )
        return {}

    items = payload.get("items", []) or []
    if not items:
        warnings.append(
            "El video no aparece en la API (puede ser privado, eliminado o de otro proyecto)."
        )
        return {}

    snippet = items[0].get("snippet", {}) or {}
    stats = items[0].get("statistics", {}) or {}
    return {
        "title": snippet.get("title"),
        "channel": snippet.get("channelTitle"),
        "channel_id": snippet.get("channelId"),
        "published_at": snippet.get("publishedAt"),
        "view_count": _safe_int(stats.get("viewCount")),
        "like_count": _safe_int(stats.get("likeCount")),
        "comment_count": _safe_int(stats.get("commentCount")),
    }


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
    # El minimo nunca puede superar el maximo: con `limit` por debajo de 24
    # (p. ej. el fallback tras un video inexistente) randrange lanzaba
    # "empty range in randrange".
    total = min(limit, rng.randint(min(max(24, limit // 2), limit), limit))
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
    # Las rafagas de bots pueden generar mas comentarios de los pedidos: se
    # recorta para respetar el limite que pidio el usuario, que es el mismo
    # contrato que cumple la ruta real de la API.
    comments = comments[:limit]
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
# 4b. EXTRACCION: archivos subidos (CSV / JSON) - multiplataforma
# ===========================================================================
# El motor no sabe de donde vienen los comentarios: solo necesita `RawComment`.
# Esta seccion traduce el vertido de TikTok, Instagram, X, Facebook, YouTube
# Studio o cualquier herramienta de exportacion al MISMO `RawComment`, para
# que el analisis sea identico al de YouTube: 40 features -> 9 heuristicas ->
# agregacion. No hay una "vía corta" para archivos: quien sube un CSV pasa
# exactamente por el mismo pipeline.
#
# Limites a proposito: esto analiza MUESTRAS (hasta 200 filas), no vertidos de
# un millon de comentarios. Un fichero de 100 MB se rechaza antes de leerlo.

UPLOAD_MAX_BYTES = 5 * 1024 * 1024       # 5 MB: suficiente para 200 filas de sobra
UPLOAD_MAX_ROWS = 200                    # el usuario pide 150-200; fijamos el techo
UPLOAD_FORMATS = ("csv", "json")

# Alias de columna -> campo canonico. Se normalizan sin acentos ni puntuacion,
# asi "Nombre de Autor" y "autor" caen en lo mismo. El orden del diccionario es
# el orden de preferencia: si hay "text" y "comment", gana "text".
COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "author": (
        "author", "autor", "user", "usuario", "username", "user_name", "nick",
        "name", "nombre", "fullname", "handle", "screen_name", "author_name",
        "by", "channel", "creator", "cuenta", "perfil", "comentarista",
    ),
    "text": (
        "text", "texto", "comment", "comentario", "message", "mensaje",
        "content", "contenido", "body", "caption", "comment_text",
        "comentario_texto", "message_text", "review", "publicacion", "post",
        "tweet", "description", "desc", "comentario_text",
    ),
    "date": (
        "date", "fecha", "published_at", "publish_date", "created_at",
        "created", "timestamp", "time", "datetime", "sent", "posted_at",
        "comment_date", "comentario_fecha", "full_date", "pub_date",
        "creation_time", "date_time",
    ),
    "likes": (
        "likes", "like", "like_count", "likes_count", "num_likes", "favorite_count",
        "digg_count", "reactions", "reaction_count", "hearts", "faves",
        "favorites", "fav", "upvotes", "retweets", "retweet_count",
        "me_gusta", "me_gustas",
    ),
    "replies": (
        "replies", "reply", "reply_count", "replies_count", "respuestas",
        "respuesta", "comments_count", "comment_count", "responses",
        "response_count", "resp",
    ),
    "id": (
        "id", "comment_id", "comentario_id", "cid", "tweet_id", "post_id",
        "pk", "uid", "uuid",
    ),
    "platform": (
        "platform", "plataforma", "red", "social", "origin", "network",
        "app", "app_name", "fuente",
    ),
    "is_reply": (
        "is_reply", "reply_to", "in_reply_to", "reply_to_id", "parent_id",
        "respuesta_a", "is_response",
    ),
}

# Deteccion de plataforma: por nombre de fichero, luego por valor de columna.
# "x" es ambiguo como fichero, por eso se busca por nombre de columna aparte.
PLATFORM_HINTS: tuple[tuple[str, str], ...] = (
    ("tiktok", "tiktok"),
    ("instagram", "instagram"),
    ("ig", "instagram"),
    ("facebook", "facebook"),
    ("fb", "facebook"),
    ("youtube", "youtube"),
    ("yt", "youtube"),
    ("twitter", "x"),
    ("threads", "threads"),
    ("twitch", "twitch"),
    ("reddit", "reddit"),
    ("linkedin", "linkedin"),
    ("telegram", "telegram"),
)

# Formatos de fecha que se aceptan ademas del ISO-8601. Cubren lo que exportan
# Excel, Google Sheets y las plataformas espanolas (dia/mes/ano).
DATE_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    "%d-%m-%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%d %b %Y %H:%M:%S",
    "%d %b %Y",
    "%d %B %Y",
    "%b %d, %Y %H:%M:%S",
    "%b %d, %Y",
)


def _norm_key(value: Any) -> str:
    """`Nombre de Autor` -> `nombre_autor`.

    Sin acentos ni signos: los exportadores inventan etiquetas constantemente
    y `autor`, `Autor ` y `AUTOR!` deben caer en lo mismo.
    """
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _map_columns(columns: Sequence[Any]) -> dict[str, str]:
    """Asocia cada columna del fichero a su campo canonico."""
    indexed: dict[str, Any] = {}
    for original in columns:
        key = _norm_key(original)
        if key and key not in indexed:
            indexed[key] = original
    mapping: dict[str, str] = {}
    for field_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in indexed:
                mapping[field_name] = indexed[alias]
                break
    return mapping


def _parse_int(value: Any) -> Optional[int]:
    """Entero tolerante: `+45`, `1.234`, `1,234`, `1.2K`, `3M`, `None`...

    Las exportaciones mezclan formato espanol e ingles en la misma columna; un
    `likes` mal interpretado (1.234 como 1.234 flotante) alteraria la regla de
    interaccion, asi que se normaliza aqui una sola vez.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value == value else None      # NaN != NaN
    text = str(value).strip().replace("\u00a0", " ")
    if not text or text.lower() in {"-", "n/a", "na", "null", "none", "nan", "sin datos"}:
        return None

    multiplier = 1
    suffix = text.lower().replace(" ", "")[-1:]
    if suffix in {"k", "m", "b"} and re.match(r"^\d", text.strip().lower()):
        multiplier = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[suffix]
        text = text.strip()[:-1].strip()

    text = text.replace("+", "").replace(" ", "")
    # Miles con separador: 1.234 / 1,234. Sin esto, "1.234" da 1.234 flotante.
    if re.fullmatch(r"-?\d{1,3}(?:[.,]\d{3})+", text):
        text = text.replace(".", "").replace(",", "")
    elif "," in text and "." not in text and re.fullmatch(r"-?\d+,\d+", text):
        text = text.replace(",", ".")                      # decimal espanol

    if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return None
    return int(float(text) * multiplier)


def _parse_date(value: Any) -> Optional[datetime]:
    """Fecha -> datetime ingenuo (UTC ya normalizado si la trae).

    Devuelve `None` si no se puede interpretar: mejor sin fecha que una fecha
    inventada, porque la regla temporal calcula ráfagas a partir de ellas.
    Todos los valores se normalizan al mismo tipo (ingenuos en UTC) para que la
    resta de `enrich_collective_features` no mezcle conscientes e ingenuos.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, (int, float)):
        return _from_epoch(float(value))
    text = str(value).strip().strip('"')
    if not text or text.lower() in {"n/a", "na", "null", "none"}:
        return None

    if re.fullmatch(r"\d{10}(\.\d+)?", text):              # epoch en segundos
        return _from_epoch(float(text))
    if re.fullmatch(r"\d{13}", text):                      # epoch en milisegundos
        return _from_epoch(float(text) / 1000.0)
    if re.fullmatch(r"\d{5}(\.\d+)?", text):               # serie de Excel
        return datetime(1899, 12, 30) + timedelta(days=float(text))

    iso = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(iso)
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError:
        pass

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _from_epoch(seconds: float) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError):
        return None


def _decode_upload(raw: bytes) -> tuple[str, str]:
    """Bytes -> texto, con BOM y codificaciones de Excel en espanol."""
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16"), "utf-16"
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    # latin-1 no puede fallar, pero por si acaso: no se pierde el analisis por un byte raro.
    return raw.decode("latin-1", errors="replace"), "latin-1"


def _json_rows(text: str) -> list[dict[str, Any]]:
    """Extrae la lista de registros de un JSON (o NDJSON).

    Acepta la forma plana `[{...}]`, los envoltorios habituales de las APIs
    (`{"comments": [...]}`) y JSON Lines, que es como exportan varias
    herramientas de escaneo.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        # JSON Lines: un objeto por linea. Comun en exportes automaticos.
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                return []
            if isinstance(item, dict):
                rows.append(item)
        if len(rows) >= 2:
            return rows
        raise ValueError(
            "El archivo parece JSON pero no es valido: revisa que la sintaxis "
            "sea correcta (falta una llave o una coma final)."
        )

    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in (
            "comments", "data", "items", "results", "entries", "rows",
            "comentarios", "tweets", "posts", "nodes", "records", "values",
        ):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [row for row in candidate if isinstance(row, dict)]
        # Un unico objeto tambien es un registro valido.
        if any(_norm_key(k) in {a for aliases in COLUMN_ALIASES.values() for a in aliases}
               for k in payload):
            return [payload]
        raise ValueError(
            "El JSON no contiene ninguna lista de comentarios. Se esperaba una "
            "lista `[{...}]` o un objeto con una clave como `comments`, `data` "
            f"o `items`. Claves encontradas: {', '.join(list(payload)[:8])}."
        )
    raise ValueError("El archivo JSON no contiene una lista de comentarios.")


def _csv_rows(text: str) -> tuple[list[dict[str, Any]], str]:
    """CSV -> registros, con separador detectado (`,`, `;`, tabulador)."""
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        dialect, delimiter = csv.excel, ","

    rows = list(csv.reader(io.StringIO(text), dialect))
    rows = [row for row in rows if any(str(cell).strip() for cell in row)]
    if not rows:
        raise ValueError("El archivo CSV esta vacio.")

    header = [str(cell).lstrip("\ufeff").strip() for cell in rows[0]]
    normalized = {_norm_key(cell) for cell in header}
    known = {alias for aliases in COLUMN_ALIASES.values() for alias in aliases}
    if not (normalized & known):
        raise ValueError(
            "No se reconoce la cabecera del CSV: se encontraron las columnas "
            f"[{', '.join(header[:12])}] y ninguna coincide con autor, texto, "
            "fecha o likes."
        )

    records = []
    for line in rows[1:]:
        record = {header[i]: (line[i] if i < len(line) else None) for i in range(len(header))}
        records.append(record)
    return records, delimiter


def _detect_platform(filename: str, mapping: dict[str, str],
                     records: Sequence[dict[str, Any]]) -> str:
    """Plataforma de origen: indicada en el formulario > columna > nombre.

    Los indicios cortos («ig», «x») solo valen como PALABRA suelta: si se
    buscaran como subcadena, «max.csv» se detectaria como X y «digit.csv» como
    Instagram. Los largos si valen como subcadena para cubrir
    «comentarios_tiktok_2026.csv», donde TikTok esta junto a un guion bajo.
    """
    def matches(hint: str, lowered: str, tokens: set[str]) -> bool:
        if len(hint) <= 3:
            return hint in tokens
        return hint in lowered

    def tokens_of(value: str) -> set[str]:
        return set(re.split(r"[^a-z0-9]+", value))

    lowered_file = (filename or "").lower()
    file_tokens = tokens_of(lowered_file)
    for hint, label in PLATFORM_HINTS:
        if matches(hint, lowered_file, file_tokens):
            return label

    column = mapping.get("platform")
    if column:
        for record in records[:25]:
            value = _norm_key(record.get(column))
            for hint, label in PLATFORM_HINTS:
                if matches(hint, value, tokens_of(value)):
                    return label
            if value in {"otra", "otro", "other", "custom"}:
                return "otra"
    return "otra"


def parse_uploaded_comments(
    filename: str,
    raw: bytes,
    platform: Optional[str] = None,
    limit: int = UPLOAD_MAX_ROWS,
) -> Dataset:
    """CSV/JSON de comentarios -> `Dataset`, listo para `score_dataset()`.

    Toda la validacion ocurre aqui y devuelve mensajes accionables: quien sube
    un vertido de TikTok no puede leer el stack del servidor para saber que la
    cabecera no tenia columna de texto.
    """
    if not raw:
        raise HTTPException(status_code=422, detail="El archivo esta vacio.")
    if len(raw) > UPLOAD_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"El archivo pesa {len(raw):,} bytes y el maximo aceptado es "
                   f"{UPLOAD_MAX_BYTES:,} bytes ({UPLOAD_MAX_BYTES // (1024 * 1024)} MB). "
                   "Analiza una muestra, no el vertido completo.",
        )

    suffix = Path(filename or "").suffix.lower().lstrip(".")
    if suffix and suffix not in UPLOAD_FORMATS:
        raise HTTPException(
            status_code=415,
            detail=f"Formato «.{suffix}» no soportado. Sube un archivo "
                   f"{' o '.join(fmt.upper() for fmt in UPLOAD_FORMATS)} exportado de la red social.",
        )

    text, encoding = _decode_upload(raw)
    looks_json = text.lstrip("﻿ \t\r\n").startswith(("[", "{"))

    delimiter = None
    if not suffix and not looks_json:
        raise HTTPException(
            status_code=415,
            detail="No se pudo determinar el formato del archivo. Sube un CSV o un JSON "
                   "con la extension correspondiente.",
        )

    try:
        if looks_json or suffix == "json":
            records = _json_rows(text)
            fmt = "json"
        else:
            records, delimiter = _csv_rows(text)
            fmt = "csv"
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Ordenador a proposito: si el archivo no tiene filas, no hay columnas que
    # mostrar, y el mensaje de error no debe romperse al intentar listarlas.
    columns = list(records[0].keys()) if records else []
    if not records:
        raise HTTPException(
            status_code=422,
            detail="El archivo no contiene ninguna fila de datos. "
                   "Se esperaba una cabecera y al menos un comentario.",
        )
    mapping = _map_columns(columns)
    if "text" not in mapping:
        raise HTTPException(
            status_code=422,
            detail="No se encontro ninguna columna de texto. Se encontraron: "
                   f"[{', '.join(columns[:12])}]. Renombra la columna del "
                   "comentario a «text», «texto» o «comment».",
        )

    warnings: list[str] = []
    if "author" not in mapping:
        warnings.append(
            "El archivo no incluye columna de autor: se analiza como «anonimo». "
            "La regla de patron de identidad no puede evaluarse."
        )
    if "date" not in mapping:
        warnings.append(
            "El archivo no incluye columna de fecha: la regla temporal y la "
            "deteccion de ráfagas se evaluan sin datos temporales."
        )
    if "likes" not in mapping:
        warnings.append(
            "El archivo no incluye columna de likes: la regla de interaccion "
            "solo usa enlaces y respuestas."
        )

    rows_in_file = len(records)
    records = records[: max(limit, 1)]
    if rows_in_file > len(records):
        warnings.append(
            f"Se analizaron {len(records)} de {rows_in_file} filas del archivo "
            f"(limite de muestra: {UPLOAD_MAX_ROWS} comentarios)."
        )

    comments: list[RawComment] = []
    skipped = 0
    for index, record in enumerate(records):
        body = record.get(mapping["text"])
        body_text = str(body).strip() if body is not None else ""
        if not body_text:
            skipped += 1
            continue
        author_value = record.get(mapping.get("author")) if "author" in mapping else None
        author = str(author_value).strip() if author_value not in (None, "") else "anonimo"
        published = _parse_date(record.get(mapping["date"])) if "date" in mapping else None
        likes = _parse_int(record.get(mapping["likes"])) if "likes" in mapping else None
        replies = _parse_int(record.get(mapping.get("replies"))) if "replies" in mapping else None
        comment_id_value = record.get(mapping.get("id")) if "id" in mapping else None
        is_reply_value = record.get(mapping.get("is_reply")) if "is_reply" in mapping else None

        comments.append(RawComment(
            comment_id=str(comment_id_value) if comment_id_value not in (None, "") else f"file-{index:05d}",
            author=author[:120],
            text=body_text,
            published_at=published,
            like_count=likes,
            reply_count=replies if replies is not None else 0,
            # En otras redes toda cuenta tiene perfil: es el equivalente a
            # `has_channel` de YouTube y sin esto la regla de identidad
            # castigaria a todos los usuarios por no tener "canal".
            has_channel=True,
            depth=1 if _truthy(is_reply_value) else 0,
        ))

    if not comments:
        raise HTTPException(
            status_code=422,
            detail="Ninguna fila del archivo tiene texto de comentario. "
                   f"Se leyeron {rows_in_file} filas y ninguna aportaba contenido.",
        )
    if skipped:
        warnings.append(f"{skipped} filas sin texto fueron ignoradas.")

    detected = _detect_platform(filename, mapping, records)
    label = (platform or "").strip()[:40] or detected
    if _norm_key(label) in {"auto", "automatica", "unknown", "ninguna"}:
        label = detected

    file_size = len(raw)
    return Dataset(
        comments=comments,
        source="file_upload",
        warnings=warnings,
        video_meta={
            "title": filename or "archivo",
            "platform": label,
            "is_upload": True,
            "is_demo": False,
            "channel": label,
            "comment_count": len(comments),
        },
        api_info={
            "engine": "archivo",
            "file_name": filename,
            "file_size": file_size,
            "format": fmt,
            "encoding": encoding,
            "delimiter": delimiter,
            "rows_in_file": rows_in_file,
            "rows_used": len(comments),
            "platform": label,
            "columns_found": columns,
            "column_mapping": mapping,
            "trace": (
                f"{fmt.upper()} · {encoding}"
                + (f" · separador «{delimiter}»" if delimiter else "")
                + f" · {rows_in_file} filas leidas -> {len(comments)} analizadas "
                + "-> 40 features -> 9 heuristicas"
            ),
        },
    )


def _truthy(value: Any) -> bool:
    """Interpreta indicadores de «es respuesta» de exportaciones ajenas."""
    if value is None or value == "":
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return _norm_key(value) in {"true", "1", "si", "s", "yes", "y", "reply", "respuesta"}


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
# 6. MOTOR HEURISTICO (9 reglas ponderadas)
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
    """Aplica las 9 heuristicas y devuelve el veredicto con su evidencia."""
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
    elif source == "file_upload":
        findings.append(
            "Analisis sobre archivo subido: los comentarios se leyeron de un CSV/JSON "
            "proporcionado por el usuario y pasaron por el mismo motor de 9 heuristicas "
            "que el modo YouTube. Revisa que el mapeo de columnas refleje tu archivo."
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
    elif source == "file_upload":
        action += " (Calculado sobre un archivo subido, no sobre la plataforma en vivo.)"

    return {"level": level, "label": label, "action": action}


# ===========================================================================
# 8. ORQUESTACION
# ===========================================================================

def score_dataset(dataset: Dataset) -> list[Verdict]:
    """Fase 2 + 3 del pipeline: features -> colectivo -> score.

    Se extrae como funcion propia porque la demo en vivo y el analisis normal
    deben producir EXACTAMENTE los mismos veredictos: si el motor se duplicara,
    la demo dejaria de ser una prueba valida de la deteccion real.
    """
    features = [build_features(comment) for comment in dataset.comments]
    # Necesario antes de puntuar: la similitud maxima y los duplicados de autor
    # son rasgos de_dataset, no de comentario suelto.
    enrich_collective_features(features)
    return [score_comment(f) for f in features]


def comment_payload(verdict: Verdict) -> dict[str, Any]:
    """Serializa un veredicto al mismo formato exacto que devuelve la API."""
    f = verdict.features
    return {
        "comment_id": f.comment_id,
        "author": f.author,
        "text_preview": f.text_preview,
        "published_at": f.published_at,
        "bot_score": verdict.bot_score,
        "verdict": verdict_label(verdict.bot_score),
        "is_reply": f.depth == 1,
        "reasons": verdict.reasons,
        "features": {
            "text_length": f.text_length,
            "word_count": f.word_count,
            "unique_word_ratio": f.unique_word_ratio,
            "emoji_count": f.emoji_count,
            "link_count": f.link_count,
            "spam_score": f.spam_score,
            "spam_terms": f.spam_terms,
            "uppercase_ratio": f.uppercase_ratio,
            "char_entropy": f.char_entropy,
            "posting_hour_utc": f.posting_hour_utc,
            "author_duplicate_count": f.author_duplicate_count,
            "duplicate_ratio": f.duplicate_ratio,
            "max_similarity": f.max_similarity,
            "like_count": f.like_count,
            "reply_count": f.reply_count,
            "has_channel": f.has_channel,
        },
    }


def build_analysis(
    video_id: str,
    dataset: Dataset,
    verdicts: list[Verdict],
    include_comments: bool = True,
    demo: bool = False,
    source_url: Optional[str] = None,
) -> AnalyzeResponse:
    """Fase 4: agrega los veredictos y arma el `AnalyzeResponse`.

    `source_url` permite sobrescribir el enlace canonico: los archivos subidos
    y las demos no son un video de YouTube, y publicar un `youtu.be` inventado
    en la respuesta seria una mentira.
    """
    metrics, signals, feature_averages = aggregate(verdicts, dataset.source)
    profile = risk_profile(metrics["bot_percentage"], dataset.source)
    metrics["risk_level"] = profile["level"]

    findings = build_findings(metrics, signals, dataset.source)
    ordered = sorted(verdicts, key=lambda v: v.bot_score, reverse=True)

    if source_url is None:
        source_url = canonical_url(video_id) if not demo else ""

    return AnalyzeResponse(
        analysis_id=uuid.uuid4().hex[:12],
        generated_at=datetime.now(timezone.utc).isoformat(),
        app_version=settings.app_version,
        dataset_source=dataset.source,
        source={
            "video_id": video_id,
            "url": source_url,
            **dataset.video_meta,
        },
        api=dataset.api_info,
        metrics=metrics,
        risk=profile,
        distribution={
            "high": sum(1 for v in verdicts if v.bot_score >= HIGH_THRESHOLD),
            "medium": sum(1 for v in verdicts
                          if MEDIUM_THRESHOLD <= v.bot_score < HIGH_THRESHOLD),
            "low": sum(1 for v in verdicts if v.bot_score < MEDIUM_THRESHOLD),
        },
        signals=signals,
        feature_averages=feature_averages,
        findings=findings,
        comments=[comment_payload(v) for v in (ordered if include_comments else [])],
        warnings=dataset.warnings,
    )


def run_analysis(video_id: str, limit: int, include_comments: bool = True) -> AnalyzeResponse:
    """Pipeline completo: extraccion -> features -> scoring -> agregacion."""
    dataset = fetch_youtube_comments(video_id, limit)
    if not dataset.comments:
        raise HTTPException(
            status_code=422,
            detail="No se obtuvo ningun comentario para analizar. Prueba con otro video.",
        )
    verdicts = score_dataset(dataset)
    return build_analysis(video_id, dataset, verdicts, include_comments=include_comments)


def verdict_label(score: float) -> str:
    if score >= HIGH_THRESHOLD:
        return "high"
    if score >= MEDIUM_THRESHOLD:
        return "medium"
    return "low"


# ===========================================================================
# 9. DEMO EN VIVO (Server-Sent Events)
# ===========================================================================
# El enunciado pide una feria con un "muro que se actualiza solo". Este endpoint
# es esa pared, sin instalar WebSockets: SSE viaja sobre HTTP plano, atraviesa
# proxies sin configuracion extra y lo consume `EventSource` en el navegador.
#
# Decisiones de diseño:
#   * No hay logica de scoring propia. Se generan comentarios con el MISMO
#     `simulate_comments()` y se puntuan con el MISMO `score_dataset()`, asi que
#     lo que se ve en la demo es literalmente lo que haria con datos reales.
#   * El dataset se calcula COMPLETO antes de empezar a emitir. Emitirlo a
#     medida se generaria obligando a recalcular las features colectivas
#     (similitud maxima, duplicados de autor) en cada comentario, y los
#     veredictos del final no cuadrarian con los intermedios.
#   * Cada evento lleva las metricas acumuladas para que el panel pueda
#     actualizar el porcentaje de bots sin esperar al evento final.

DEMO_TITLES = (
    "Ataque de bots en directo",
    "Inundacion de spam en curso",
    "Red de cuentas automatizadas",
    "Campaña de enlaces coordinados",
)

# Fases que el frontend muestra mientras espera, para que el usuario entienda
# que esta pasando en vez de ver un spinner mudo.
DEMO_PHASES = (
    "Generando comentarios sintéticos...",
    "Extrayendo 40 features por comentario...",
    "Aplicando 9 heurísticas ponderadas...",
    "Calculando riesgo y distribucion...",
)


def build_demo_dataset(count: int, seed: Optional[str] = None) -> tuple[str, Dataset]:
    """Dataset de demostracion: ataque de bots generado en memoria.

    No toca la red ni consume cuota, asi que la demo funciona aunque no haya
    `YOUTUBE_API_KEY` configurada (requisito para poder hacer la demo en una
    feria recien desplegada en Render).
    """
    rng = random.Random(f"demo:{seed or uuid.uuid4().hex[:8]}")
    video_id = f"demo-{rng.getrandbits(30):010d}"
    dataset = simulate_comments(video_id, count)
    dataset.video_meta = {
        **dataset.video_meta,
        "title": f"{rng.choice(DEMO_TITLES)} · ronda {rng.randint(1000, 9999)}",
        "is_demo": True,
    }
    dataset.warnings = [
        "DEMO en vivo: comentarios sinteticos generados en memoria. "
        "No proceden de YouTube, no gastan cuota y no demuestran nada sobre "
        "ningun video real.",
    ]
    return video_id, dataset


def _sse(event: str, data: dict[str, Any]) -> str:
    """Serializa un evento SSE. `ensure_ascii=False` para no escapar los emojis."""
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _running_totals(verdicts: list[Verdict], up_to: int) -> dict[str, Any]:
    """Metricas acumuladas del mural en vivo (barato: solo sobre los emitidos).

    La terminologia es la MISMA que usa `aggregate()`, a proposito:
      * `suspected_bot_comments` = high + medium, no solo high.
      * `bot_percentage` sale de la MEDIA de scores por el amplificador de
        cluster, no de contar comentarios sobre un umbral. Por eso aqui se
        publica `average_score` (la base de ese porcentaje) y no un ratio,
        que dejaria un numero que contradiria al cierre del analisis.
    """
    seen = verdicts[:up_to]
    total = len(seen)
    if not total:
        return {"analyzed": 0, "suspected": 0, "average_score": 0.0,
                "high": 0, "medium": 0, "low": 0}
    high = sum(1 for v in seen if v.bot_score >= HIGH_THRESHOLD)
    medium = sum(1 for v in seen
                 if MEDIUM_THRESHOLD <= v.bot_score < HIGH_THRESHOLD)
    return {
        "analyzed": total,
        "suspected": high + medium,
        "average_score": round(sum(v.bot_score for v in seen) / total, 1),
        "high": high,
        "medium": medium,
        "low": total - high - medium,
    }


async def _demo_event_stream(
    request: Request,
    video_id: str,
    dataset: Dataset,
    verdicts: list[Verdict],
    delay: float,
) -> Any:
    """Generador asincrono que emite el ataque comentario a comentario."""
    yield _sse("start", {
        "phases": list(DEMO_PHASES),
        "total": len(verdicts),
        "source": {"video_id": video_id, **dataset.video_meta},
        "warnings": dataset.warnings,
    })

    running = _running_totals(verdicts, 0)
    for index, verdict in enumerate(verdicts, start=1):
        # Si el usuario cierra la pestaña o pulsa "parar", se corta el stream
        # en vez de seguir calculando y acumulando en un socket muerto.
        if await request.is_disconnected():
            return
        running = _running_totals(verdicts, index)
        yield _sse("comment", {
            "index": index,
            "total": len(verdicts),
            "comment": comment_payload(verdict),
            "running": running,
        })
        # El ultimo comentario no espera: ya no hay nada mas que enviar.
        if index < len(verdicts):
            await asyncio.sleep(delay)

    # Se incluyen los comentarios para que el frontend pueda pintar el
    # histograma de scores y reutilizar el render normal sin una ruta aparte.
    analysis = build_analysis(video_id, dataset, verdicts, include_comments=True, demo=True)
    yield _sse("done", json.loads(analysis.model_dump_json()))


# ===========================================================================
# 10. FASTAPI
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
    response = templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "app_name": settings.app_name,
            "app_version": settings.app_version,
            "data_mode": "real" if settings.youtube_api_key else "simulated",
            "default_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        },
    )
    # La interfaz lleva el JS embebido: si el navegador cachea una version
    # antigua, sigue ejecutando el codigo viejo y los errores que muestra no
    # coinciden con el servidor. Sin cabecera de cache no hay desajuste.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


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
    has_key = bool((settings.youtube_api_key or "").strip())
    return {
        "app_name": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment,
        "data_mode": "youtube_api" if has_key else "simulated",
        "max_comments": settings.max_comments,
        "supported_platforms": [
            "youtube", "tiktok", "instagram", "x", "facebook", "threads",
            "twitch", "reddit", "linkedin", "telegram", "otra",
        ],
        "youtube_api": {
            "configured": has_key,
            "endpoint": YOUTUBE_API_URL,
            "max_api_pages": settings.max_api_pages,
            "quota_units_per_page": settings.api_quota_per_page,
            # Nunca se devuelve la clave, solo si existe.
            "key_hint": f"...{(settings.youtube_api_key or '')[-4:]}" if has_key else None,
        },
        "scoring": {
            "heuristics": [{"key": r.key, "label": r.label, "weight": r.weight} for r in RULES],
            "high_threshold": HIGH_THRESHOLD,
            "medium_threshold": MEDIUM_THRESHOLD,
        },
        "demo": {
            "available": True,
            "stream_url": "/api/demo/stream",
            "analyze_url": "/api/demo/live",
            "phases": list(DEMO_PHASES),
            "requires_api_key": False,
        },
        "file_upload": {
            "enabled": True,
            "endpoint": "/api/analyze/upload",
            "formats": list(UPLOAD_FORMATS),
            "max_bytes": UPLOAD_MAX_BYTES,
            "max_rows": UPLOAD_MAX_ROWS,
            "recognized_columns": {
                field_name: list(aliases) for field_name, aliases in COLUMN_ALIASES.items()
            },
            "requires_api_key": False,
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


@app.post("/api/analyze/upload", response_model=AnalyzeResponse, tags=["analisis"])
async def analyze_upload(
    file: UploadFile = File(..., description="CSV o JSON con comentarios exportados de cualquier red social."),
    platform: Optional[str] = Form(None, description="Plataforma de origen (tiktok, instagram, x...). Autodetectada si se omite."),
    include_comments: bool = Form(True, description="Incluir el detalle por comentario en la respuesta."),
    comment_limit: Optional[int] = Form(None, ge=1, le=UPLOAD_MAX_ROWS, description="Maximo de filas a analizar."),
) -> AnalyzeResponse:
    """Analiza un archivo CSV/JSON de comentarios exportados (multiplataforma).

    Es la via para analizar TikTok, Instagram, X, Facebook o cualquier otra
    red social sin dependencia de su API: se sube la muestra y pasa por el
    MISMO pipeline que YouTube (40 features -> 9 heuristicas -> agregacion).

    Ejemplo con curl:

        curl -X POST http://localhost:8000/api/analyze/upload \\
             -F "file=@comentarios.csv" -F "platform=tiktok"

    Errores accionables: 413 (peso), 415 (formato), 422 (cabecera o contenido).
    """
    raw = await file.read(UPLOAD_MAX_BYTES + 1)
    dataset = await asyncio.to_thread(
        parse_uploaded_comments,
        file.filename or "",
        raw,
        platform,
        comment_limit or UPLOAD_MAX_ROWS,
    )
    verdicts = await asyncio.to_thread(score_dataset, dataset)
    return build_analysis(
        dataset.video_meta.get("title", "archivo"),
        dataset,
        verdicts,
        include_comments=include_comments,
        source_url="",
    )


@app.post("/api/demo/live", response_model=AnalyzeResponse, tags=["demo"])
async def demo_live(
    comment_limit: int = Query(40, ge=10, le=120, description="Comentarios de la demo."),
) -> AnalyzeResponse:
    """Analisis instantaneo de un ataque de bots simulado, sin pedir URL.

    Es el boton "Simular ataque de bots" de la interfaz. Devuelve el mismo
    `AnalyzeResponse` que `/api/analyze`, asi que el frontend reutiliza todo el
    render. No consume cuota de la API ni requiere `YOUTUBE_API_KEY`.
    """
    video_id, dataset = build_demo_dataset(comment_limit)
    verdicts = await asyncio.to_thread(score_dataset, dataset)
    return build_analysis(video_id, dataset, verdicts, include_comments=True, demo=True)


@app.get("/api/demo/stream", tags=["demo"])
async def demo_stream(
    request: Request,
    comment_limit: int = Query(30, ge=4, le=120, description="comentarios a emitir."),
    delay: float = Query(0.28, ge=0.0, le=2.0, description="segundos entre comentarios."),
) -> StreamingResponse:
    """Emite un ataque de bots en vivo por Server-Sent Events.

    Eventos, en orden:
      * `start`   -> fases del pipeline, total de comentarios y avisos.
      * `comment` -> un comentario con su veredicto y las metricas acumuladas
                    (`analyzed`, `suspected`, `average_score`, `high`,
                    `medium`, `low`). Nota: `average_score` es la base del
                    `bot_percentage` final; el cierre aplica ademas el
                    amplificador de cluster sobre el conjunto completo.
      * `done`    -> el `AnalyzeResponse` completo (mismo contrato que la API).

    Cada evento lleva `data:` con JSON. El cliente lo consume con `EventSource`.
    """
    video_id, dataset = build_demo_dataset(comment_limit)
    # El calculo es CPU-bound (features + pandas): fuera del event loop para no
    # bloquear al resto de peticiones del servidor mientras se prepara la demo.
    verdicts = await asyncio.to_thread(score_dataset, dataset)

    return StreamingResponse(
        _demo_event_stream(request, video_id, dataset, verdicts, delay),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "Connection": "keep-alive",
            # Render pone nginx delante: sin esto acumula el stream en un buffer
            # y el navegador recibe los eventos a rafagas al final.
            "X-Accel-Buffering": "no",
        },
    )


def _resolve_port() -> int:
    """Puerto de escucha.

    Las PaaS (Render, Railway, Fly.io...) inyectan el puerto asignado en la
    variable `PORT` y souvent es aleatorio. Fijar 8000 a pelo hace que el
    contenedor levante pero el proxy no llegue a encontrarlo.
    """
    raw = os.environ.get("PORT", "").strip()
    if raw.isdigit():
        return int(raw)
    return 8000


# Ejecuta con:  python app.py            (usa $PORT)
# Desarrollo:    uvicorn app:app --reload
# Docker:        docker compose up --build
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",           # obligatorio en contenedores: si no, escucha solo en loopback
        port=_resolve_port(),
        # Necesario detras del proxy de Render: sin esto `request.client.ip` es
        # la IP del proxy y los redireccionamientos / cookies de sesion fallan.
        proxy_headers=True,
        forwarded_allow_ips="*",
        reload=settings.environment == "development",
    )