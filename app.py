"""SocialBotGuard - Escaner y Simulador de Moderacion en Tiempo Real.

Detecta bots y spam en un flujo de comentarios SIMULADO (o escrito a mano) con
tres heuristicas ponderadas:

  1. Frecuencia (ventana deslizante): mas de `frequency_limit` comentarios en
     `frequency_window` segundos. Penalizacion PROGRESIVA: cuanto mas se pasa
     del limite, mas sube el riesgo, y a partir de un margen se activa una
     ALERTA ROJA de spam.                                       -> +40 % (o mas)
  2. Similitud (difflib.SequenceMatcher + Jaccard)               -> +50 %
     Se dispara si el mayor de los dos ratios supera el umbral configurable.
  3. Patrones sospechosos (varios enlaces o MAYUSCULAS)          -> +20 %

Si el riesgo acumulado llega a 70 % o mas, el mensaje se marca como
[BOT DETECTADO] con la razon. Todo el estado vive en memoria: no se usan datos
reales ni APIs externas.

El umbral de similitud y la intensidad del flujo se ajustan EN VIVO desde el
panel (sliders) y via WebSocket.

Ejecuta con:  python app.py            (lee $PORT)
Desarrollo:   uvicorn app:app --reload
Docker:       docker compose up --build
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import re
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"

APP_NAME = os.environ.get("APP_NAME", "SocialBotGuard")
APP_VERSION = os.environ.get("APP_VERSION", "2.0.0")

# --- Pesos y umbrales -------------------------------------------------------
FREQUENCY_RISK = 40           # +40 % base por exceder el limite
FREQUENCY_STEP = 10           # +10 % por cada mensaje extra en la ventana
FREQUENCY_MAX_RISK = 60       # tope de la regla de frecuencia
SPAM_ALERT_MARGIN = 2         # limite + margen => alerta roja automatica

SIMILARITY_RISK = 50          # +50 %
PATTERN_RISK = 20             # +20 % (enlaces multiples o mayusculas)
UPPERCASE_MIN_LETTERS = 10    # longitud minima para evaluar mayusculas
UPPERCASE_RATIO = 0.60        # proporcion de mayusculas considerada "grita"

BOT_THRESHOLD = 70            # >= 70 % => [BOT DETECTADO]

HISTORY_LIMIT = 200           # mensajes que conserva la memoria
SNAPSHOT_LIMIT = 60           # mensajes que se reenvian al conectar

# --- Valores por defecto y rangos de la configuracion en vivo --------------
DEFAULTS = {
    "similarity_threshold": 0.80,
    "stream_speed": 1.0,
    "frequency_window": 5.0,
    "frequency_limit": 3,
}
RANGES = {
    "similarity_threshold": (0.60, 0.90),
    "stream_speed": (0.25, 3.0),
    "frequency_window": (2.0, 15.0),
    "frequency_limit": (2, 10),
}

URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)
TOKEN_RE = re.compile(r"[a-z0-9]+")


# ---------------------------------------------------------------------------
# Reglas puras (testeables sin estado)
# ---------------------------------------------------------------------------
def count_urls(text: str) -> int:
    """Numero de enlaces http/https presentes en el texto."""
    return len(URL_RE.findall(text))


def is_uppercase_blast(text: str) -> bool:
    """True si el texto es casi todo mayusculas (con suficiente texto).

    Ignora numeros, signos y espacios: solo cuenta letras. Exige un minimo de
    letras para no marcar mensajes cortos como "JAJA".
    """
    letters = [c for c in text if c.isalpha()]
    if len(letters) < UPPERCASE_MIN_LETTERS:
        return False
    uppers = sum(1 for c in letters if c.isupper())
    return (uppers / len(letters)) >= UPPERCASE_RATIO


def text_similarity(a: str, b: str) -> float:
    """Ratio de similitud [0..1] entre dos cadenas (difflib.SequenceMatcher)."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _tokens(text: str) -> set[str]:
    """Conjunto de palabras normalizadas (minusculas, alfanumericas)."""
    return set(TOKEN_RE.findall((text or "").lower()))


def jaccard_similarity(a: str, b: str) -> float:
    """Similitud de Jaccard [0..1] entre los conjuntos de palabras.

    Complementa a SequenceMatcher: es mas robusta a reordenaciones y a cambios
    de puntuacion/emojis, tipico de los bots que varian el texto.
    """
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def similarity_score(a: str, b: str) -> tuple[float, float, float]:
    """Devuelve (score, seq_ratio, jaccard). score = max(seq, jaccard)."""
    seq = text_similarity(a, b)
    jac = jaccard_similarity(a, b)
    return max(seq, jac), seq, jac


# ---------------------------------------------------------------------------
# Estado por usuario, mensaje y veredicto
# ---------------------------------------------------------------------------
@dataclass
class UserState:
    name: str
    timestamps: deque[float] = field(default_factory=lambda: deque(maxlen=64))
    texts: deque[str] = field(default_factory=lambda: deque(maxlen=12))
    blocked: bool = False


@dataclass
class Verdict:
    risk: int
    is_bot: bool
    matched: list[str]
    reason: Optional[str]
    similarity: float = 0.0
    spam_alert: bool = False


@dataclass
class Message:
    id: int
    user: str
    text: str
    timestamp: float
    risk: int
    is_bot: bool
    reason: Optional[str]
    similarity: float = 0.0
    spam_alert: bool = False
    blocked: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "user": self.user,
            "text": self.text,
            "timestamp": round(self.timestamp, 3),
            "risk": self.risk,
            "is_bot": self.is_bot,
            "reason": self.reason,
            "similarity": round(self.similarity, 3),
            "spam_alert": self.spam_alert,
            "blocked": self.blocked,
        }


# ---------------------------------------------------------------------------
# Motor heuristico
# ---------------------------------------------------------------------------
class BotEngine:
    """Aplica las tres reglas a cada mensaje y acumula el riesgo por usuario."""

    def __init__(
        self,
        similarity_threshold: float = DEFAULTS["similarity_threshold"],
        frequency_window: float = DEFAULTS["frequency_window"],
        frequency_limit: int = DEFAULTS["frequency_limit"],
    ) -> None:
        self.similarity_threshold = similarity_threshold
        self.frequency_window = frequency_window
        self.frequency_limit = frequency_limit
        self.users: dict[str, UserState] = {}
        # (usuario, texto) de los ultimos mensajes del chat, para la similitud.
        self.recent: deque[tuple[str, str]] = deque(maxlen=HISTORY_LIMIT)

    def user(self, name: str) -> UserState:
        state = self.users.get(name)
        if state is None:
            state = UserState(name=name)
            self.users[name] = state
        return state

    def analyze(self, user: str, text: str, now: Optional[float] = None) -> Verdict:
        """Evalua un comentario y devuelve un Verdict con el desglose."""
        now = time.time() if now is None else now
        state = self.user(user)
        risk = 0
        matched: list[str] = []
        reasons: list[str] = []
        spam_alert = False

        # --- Regla 1: frecuencia por ventana deslizante (progresiva) --------
        state.timestamps.append(now)
        recent = [t for t in state.timestamps if now - t < self.frequency_window]
        count = len(recent)
        if count > self.frequency_limit:
            extra = count - self.frequency_limit - 1
            freq_risk = min(FREQUENCY_RISK + extra * FREQUENCY_STEP, FREQUENCY_MAX_RISK)
            risk += freq_risk
            matched.append("frecuencia")
            if count >= self.frequency_limit + SPAM_ALERT_MARGIN:
                spam_alert = True
                reasons.append(
                    f"Rafaga de {count} mensajes en menos de {self.frequency_window:.0f} s"
                )
            else:
                reasons.append("Mensajes repetidos muy rapido")

        # --- Regla 2: similitud (SequenceMatcher + Jaccard) -----------------
        # Se compara con los mensajes previos del mismo usuario y con el chat
        # reciente, SIEMPRE antes de registrar el texto actual.
        score = seq = jac = 0.0
        for prev in state.texts:
            s, sq, jc = similarity_score(text, prev)
            if s > score:
                score, seq, jac = s, sq, jc
        for other_user, prev in self.recent:
            if other_user != user:
                s, sq, jc = similarity_score(text, prev)
                if s > score:
                    score, seq, jac = s, sq, jc
        if score > self.similarity_threshold:
            risk += SIMILARITY_RISK
            matched.append("similitud")
            reasons.append(
                f"Alta similitud de texto (Seq {seq * 100:.0f} % / Jaccard {jac * 100:.0f} %)"
            )

        # El texto entra en la memoria DESPUES de compararlo consigo mismo.
        state.texts.append(text)
        self.recent.append((user, text))

        # --- Regla 3: patrones sospechosos ----------------------------------
        if count_urls(text) >= 2:
            risk += PATTERN_RISK
            matched.append("patrones")
            reasons.append("Varios enlaces en un mensaje")
        elif is_uppercase_blast(text):
            risk += PATTERN_RISK
            matched.append("patrones")
            reasons.append("Texto en mayusculas sostenidas")

        risk = min(risk, 100)
        is_bot = risk >= BOT_THRESHOLD
        reason = " + ".join(reasons) if reasons else None
        return Verdict(risk, is_bot, matched, reason, round(score, 3), spam_alert)


# ---------------------------------------------------------------------------
# Pools dinamicos (banco de perfiles y comentarios)
# ---------------------------------------------------------------------------
HUMAN_NAMES = [
    "maria_lp", "jorge.rv", "Ana22", "el_tio_paco", "sofia_q", "dev_mateo",
    "luciana", "carlos_uy", "nina_k", "pau_ferrer", "marta_dv", "pedro_andres",
    "laura.gz", "david_mx", "camila_ar", "andres_pe", "valen_r", "santi_ok",
    "rocio_ls", "fernando.b", "julieta_88", "mateo.dev", "gonzalo_v", "ines_mr",
    "pablo.bo", "dani_serrano", "clara_nt", "hugo_mp", "elena_ruiz", "tomeu_bcn",
    "rafa_gt", "silvia_c", "nico_fm", "agus_mdq", "beto_lpz", "luna_star",
    "kevin_rd", "paula.gg", "noe_lg", "alejandro.s", "mica_tf", "german_k",
    "tati_lp", "fabio_mza", "cris_vl", "juli_rp", "sergio_ac", "vero_mdq",
]

# Comentarios humanos: saludos, modismos, preguntas, reacciones y emojis.
HUMAN_COMMENTS = [
    "Buen video, me sirvio mucho la explicacion",
    "No entiendo la parte del minuto 3, alguien me ayuda?",
    "jajaja me rei mucho con el final",
    "Primera vez por aqui, muy buen contenido",
    "Alguien sabe que cancion suena al inicio?",
    "Gracias por compartir, lo guardo para despues",
    "Muy claro todo, me suscribo",
    "Justo lo que estaba buscando, gracias!",
    "Yo lo probe ayer y funciona bien",
    "Alguien mas noto el detalle del minuto 1:20?",
    "Excelente explicacion, se nota el trabajo",
    "Vengo del directo, gran comunidad",
    "No estoy del todo de acuerdo con una parte, pero buen video",
    "Podrias hacer una segunda parte?",
    "Recomienden canales parecidos por favor",
    "Menudo pedazo de edicion, enhorabuena",
    "Me quito el sombrero, eres un crack",
    "Esto merece muchos mas likes",
    "Que buena onda explicarlo asi de simple",
    "Guardado en favoritos, volvere a verlo",
    "Amo este canal, siempre aprendo algo nuevo",
    "Mi novia y yo lo vimos juntos, nos encanto",
    "El audio se escucha genial, que microfono usas?",
    "Podrias dejar el enlace del articulo que mencionas?",
    "Desde Colombia, saludos a todos!",
    "Aqui desde Espana, gran video como siempre",
    "Muy util para principiantes, gracias de verdad",
    "Vengo de TikTok y me quedo por aqui",
    "Nunca habia entendido esto, ahora si!",
    "Estaba perdidisimo y me iluminaste jajaja",
    "Ese ejemplo final fue la guinda del pastel",
    "Lo comparti con mis companeros de trabajo",
    "A alguien mas le aparece en el minuto 4 un fallo?",
    "Haz un video sobre el tema del minuto 6 por favor",
    "Que bien explicado, sin rodeos",
    "Suscribo y activo la campanita, se lo merece",
    "Me encanta cuando resumes en una frase",
    "Gran trabajo de investigacion, se agradece",
    "Este canal es una joya escondida",
    "Por fin alguien que lo explica claro",
    "Venimos del podcast, genial el invitado",
    "Aprendi mas en 10 min que en un curso entero",
    "Alguien tiene el codigo del ejemplo?",
    "Podrias hacer directos sobre esto?",
    "Volvere a verlo con calma manana",
    "Eres el mejor, sigue asi",
    "Que edicion tan limpia, se nota el cariño",
    "No me esperaba ese giro final, que grande",
    "Gracias por tomarte el tiempo de responder dudas",
    "Lo vi en recomendados y no me arrepiento",
    "Muy buena la narrativa, engancha",
    "Te sigo desde el primer video, gran evolucion",
    "Esto deberia estar en las escuelas jaja",
    "Buenas noches, dejando mi granito de arena",
    "Hola a todos, aqui preparando la cena con el video",
    "Muy interesante, no lo conocia",
    "Un saludo desde Mexico, excelente contenido",
]

# Perfiles de bot: nombres con pinta promocional.
BOT_NAMES = [
    "promo_deals_24", "crypto_now", "x100_followers", "gana_quick",
    "free_likes_net", "investment_pro", "viral_booster", "money_fast_x",
    "link_master_01", "seguifans_pro", "easy_cash_2026", "boost.social",
]

# Comentarios de bot: variaciones sutiles del mismo mensaje (para estresar la
# similitud de Jaccard y SequenceMatcher) y mensajes de patron claro.
BOT_MESSAGES = [
    # --- Familia "gana dinero" con variaciones sutiles ---
    "Gana 500$ diarios aqui: bit.ly/promo",
    "Gana 500 USD diarios en bit.ly/promo",
    "GANATE 500 dolares al dia entrando en bit.ly/promo",
    "Gana dinero rapido  bit.ly/promo",
    "GANA DINERO RAPIDO  bit.ly/promo",
    "GANA DINERO FACIL DESDE CASA HAZ CLIC AQUI https://bit.ly/gana1",
    "Gana dinero facil desde casa, haz clic aqui  https://bit.ly/gana1",
    "GANATE 500 DOLARES AL DIA ENTRANDO EN BIT.LY/PROMO",
    # --- Familia "seguidores gratis" ---
    "SIGUEME Y TE SIGO AL INSTANTE, GANATE SEGUIDORES GRATIS",
    "Sigueme y te sigo al instante, ganate seguidores gratis",
    "Consigue seguidores gratis al instante, clic aqui ya!",
    "FREE FOLLOWERS!! CLICK THE LINK NOW FREE FOLLOWERS!!",
    "FREE FOLLOWERS CLICK THE LINK NOW FREE FOLLOWERS",
    # --- Familia "invierte / cripto" ---
    "INVIERTE HOY Y DUPLICA TU DINERO EN 24 HORAS",
    "Invierte hoy y duplica tu dinero en 24 horas",
    "CRYPTO MILLONARIO: empieza con 50 y retira 5000  t.me/crypto_now",
    "Cripto millonario: empieza con 50 y retira 5000  t.me/crypto_now",
    # --- Patron de enlaces multiples ---
    "MIRA MI PERFIL PARA MAS INFORMACION http://spam.example http://spam2.example",
    "PROMO ESPECIAL COMPRA AHORA https://t.me/ofertas https://t.me/ofertas",
]


class ShuffleBag:
    """Saca elementos al azar sin repetir hasta agotar la bolsa ("bag")."""

    def __init__(self, items: list[str]) -> None:
        self.items = list(items)
        self._bag: list[str] = []

    def next(self) -> str:
        if not self._bag:
            self._bag = self.items[:]
            random.shuffle(self._bag)
        return self._bag.pop()


# ---------------------------------------------------------------------------
# Hub de moderacion (estado global + clientes WebSocket + simulador)
# ---------------------------------------------------------------------------
class ModerationHub:
    def __init__(self) -> None:
        self.engine = BotEngine()
        self.config: dict[str, Any] = dict(DEFAULTS)
        self.messages: deque[Message] = deque(maxlen=HISTORY_LIMIT)
        self.clients: set[WebSocket] = set()
        # Bolsas dinamicas: garantizan variedad antes de repetir un texto.
        self.human_names = ShuffleBag(HUMAN_NAMES)
        self.human_comments = ShuffleBag(HUMAN_COMMENTS)
        self.bot_names = ShuffleBag(BOT_NAMES)
        self.bot_messages = ShuffleBag(BOT_MESSAGES)
        self.total = 0
        self.real = 0
        self.bots = 0
        self.blocked = 0
        self._seq = 0
        self.running = False
        self.attacking = False
        self._attack_left = 0
        self._attackers: list[str] = []
        self._task: Optional[asyncio.Task] = None
        self._send_lock = asyncio.Lock()

    # --- contadores -----------------------------------------------------
    def blocked_users(self) -> list[str]:
        return sorted(u for u, st in self.engine.users.items() if st.blocked)

    def stats(self) -> dict[str, Any]:
        real_pct = round(self.real / self.total * 100, 1) if self.total else 0.0
        bot_pct = round(self.bots / self.total * 100, 1) if self.total else 0.0
        return {
            "total": self.total,
            "real": self.real,
            "bots": self.bots,
            "blocked": self.blocked,
            "real_pct": real_pct,
            "bot_pct": bot_pct,
            "running": self.running,
            "attacking": self.attacking,
            "blocked_users": self.blocked_users(),
        }

    def state(self) -> dict[str, Any]:
        return {"running": self.running, "attacking": self.attacking}

    def config_view(self) -> dict[str, Any]:
        return dict(self.config)

    # --- configuracion en vivo -----------------------------------------
    def update_config(self, **changes: Any) -> dict[str, Any]:
        for key, value in changes.items():
            if key not in RANGES or value is None:
                continue
            lo, hi = RANGES[key]
            if key == "similarity_threshold":
                value = max(lo, min(hi, float(value)))
            elif key == "stream_speed":
                value = max(lo, min(hi, float(value)))
            elif key == "frequency_window":
                value = max(lo, min(hi, float(value)))
            elif key == "frequency_limit":
                value = int(max(lo, min(hi, int(value))))
            self.config[key] = value
        self.engine.similarity_threshold = self.config["similarity_threshold"]
        self.engine.frequency_window = self.config["frequency_window"]
        self.engine.frequency_limit = self.config["frequency_limit"]
        return dict(self.config)

    # --- difusion -------------------------------------------------------
    async def broadcast(self, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False)
        async with self._send_lock:
            dead: list[WebSocket] = []
            for ws in list(self.clients):
                try:
                    await ws.send_text(raw)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self.clients.discard(ws)

    async def send_to(self, ws: WebSocket, payload: dict[str, Any]) -> None:
        try:
            await ws.send_text(json.dumps(payload, ensure_ascii=False))
        except Exception:
            self.clients.discard(ws)

    # --- publicacion de un comentario ----------------------------------
    async def publish(self, user: str, text: str) -> Optional[Message]:
        text = (text or "").strip()
        if not text:
            return None
        state = self.engine.users.get(user)
        if state is not None and state.blocked:
            return None  # un usuario bloqueado ya no participa

        verdict = self.engine.analyze(user, text)
        self._seq += 1
        msg = Message(
            id=self._seq, user=user, text=text, timestamp=time.time(),
            risk=verdict.risk, is_bot=verdict.is_bot, reason=verdict.reason,
            similarity=verdict.similarity, spam_alert=verdict.spam_alert,
        )
        self.messages.append(msg)
        self.total += 1
        if verdict.is_bot:
            self.bots += 1
        else:
            self.real += 1

        await self.broadcast({"type": "message", "data": msg.to_dict()})
        await self.broadcast({"type": "stats", "data": self.stats()})
        return msg

    # --- acciones -------------------------------------------------------
    async def block(self, user: str) -> None:
        state = self.engine.user(user)
        if state.blocked:
            return
        state.blocked = True
        self.blocked += 1
        for msg in self.messages:
            if msg.user == user:
                msg.blocked = True
        await self.broadcast({"type": "blocked", "data": {"user": user}})
        await self.broadcast({"type": "stats", "data": self.stats()})

    async def unblock(self, user: str) -> None:
        state = self.engine.users.get(user)
        if state is None or not state.blocked:
            return
        state.blocked = False
        self.blocked = max(0, self.blocked - 1)
        for msg in self.messages:
            if msg.user == user:
                msg.blocked = False
        await self.broadcast({"type": "unblocked", "data": {"user": user}})
        await self.broadcast({"type": "stats", "data": self.stats()})

    async def reset(self) -> None:
        self.engine = BotEngine(
            similarity_threshold=self.config["similarity_threshold"],
            frequency_window=self.config["frequency_window"],
            frequency_limit=self.config["frequency_limit"],
        )
        self.messages.clear()
        self.total = self.real = self.bots = self.blocked = 0
        self._seq = 0
        self.attacking = False
        self._attack_left = 0
        self.running = False  # vuelve al estado inicial (pausado)
        self._attackers = []
        await self.broadcast({"type": "reset", "data": {}})
        await self.broadcast({"type": "state", "data": self.state()})
        await self.broadcast({"type": "stats", "data": self.stats()})

    async def set_running(self, running: bool) -> None:
        self.running = running
        await self.broadcast({"type": "state", "data": self.state()})

    async def start_attack(self, size: int = 16) -> None:
        self.attacking = True
        self._attack_left = max(size, 1)
        self.running = True
        # Un ataque coordinado sale de POCAS cuentas: asi se disparan la regla de
        # frecuencia (rafaga) y la alerta roja automatica de spam.
        self._attackers = random.sample(BOT_NAMES, k=min(2, len(BOT_NAMES)))
        await self.broadcast({"type": "state", "data": self.state()})

    # --- bucle del simulador -------------------------------------------
    def _next_stream_message(self) -> tuple[str, str]:
        # ~18 % de los mensajes normales son de bot; el resto, usuarios reales.
        if random.random() < 0.18:
            return self.bot_names.next(), self.bot_messages.next()
        return self.human_names.next(), self.human_comments.next()

    async def loop(self) -> None:
        """Bucle unico: emite el flujo normal y resuelve los ataques."""
        while True:
            try:
                speed = max(self.config["stream_speed"], 0.05)
                if self.attacking:
                    user = random.choice(self._attackers) if self._attackers else self.bot_names.next()
                    text = self.bot_messages.next()
                    await self.publish(user, text)
                    self._attack_left -= 1
                    if self._attack_left <= 0:
                        self.attacking = False
                        await self.broadcast({"type": "state", "data": self.state()})
                    await asyncio.sleep(random.uniform(0.05, 0.16) / speed)
                elif self.running:
                    user, text = self._next_stream_message()
                    await self.publish(user, text)
                    await asyncio.sleep(max(0.05, random.uniform(0.5, 1.7) / speed))
                else:
                    await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Un fallo puntual nunca debe matar el bucle de la demo.
                await asyncio.sleep(0.5)


hub = ModerationHub()


# ---------------------------------------------------------------------------
# Aplicacion FastAPI
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    hub._task = asyncio.create_task(hub.loop())
    try:
        yield
    finally:
        if hub._task is not None:
            hub._task.cancel()
            try:
                await hub._task
            except (asyncio.CancelledError, Exception):
                pass


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description=(
        "Escaner y simulador de moderacion en tiempo real.\n\n"
        "Flujo de comentarios por **WebSocket** (`/ws`) evaluado con tres "
        "heuristicas ponderadas (frecuencia por ventana deslizante, similitud "
        "de SequenceMatcher + Jaccard y patrones). El estado vive en memoria: "
        "no se usan datos reales."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index(request: Request) -> HTMLResponse:
    """Panel de control (Tailwind por CDN, JS embebido, sin build step)."""
    response = templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"app_name": APP_NAME, "app_version": APP_VERSION},
    )
    # El JS va embebido: evita que el navegador ejecute una version cacheada.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/health", tags=["sistema"])
async def health() -> dict[str, Any]:
    """Liveness probe (Docker HEALTHCHECK / Render)."""
    return {"status": "ok", "version": APP_VERSION}


@app.get("/api/config", tags=["sistema"])
async def api_config() -> dict[str, Any]:
    """Reglas, umbrales y rangos de los sliders, para que la interfaz se explique sola."""
    return {
        "app_name": APP_NAME,
        "version": APP_VERSION,
        "config": hub.config_view(),
        "ranges": {k: {"min": v[0], "max": v[1]} for k, v in RANGES.items()},
        "weights": {
            "frequency": FREQUENCY_RISK,
            "similarity": SIMILARITY_RISK,
            "patterns": PATTERN_RISK,
        },
        "bot_threshold": BOT_THRESHOLD,
        "websocket_path": "/ws",
    }


@app.get("/api/stats", tags=["moderacion"])
async def api_stats() -> dict[str, Any]:
    """Foto actual de los contadores (util para pruebas y monitorizacion)."""
    return hub.stats()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """Canal bidireccional: recibe mensajes/ordenes y empuja el muro en vivo."""
    await ws.accept()
    hub.clients.add(ws)
    await hub.send_to(ws, {
        "type": "snapshot",
        "data": {
            "stats": hub.stats(),
            "state": hub.state(),
            "config": hub.config_view(),
            "messages": [m.to_dict() for m in list(hub.messages)[-SNAPSHOT_LIMIT:]],
        },
    })
    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await hub.send_to(ws, {"type": "error", "data": {"detail": "JSON invalido"}})
                continue
            await _handle_command(ws, data)
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)


async def _handle_command(ws: WebSocket, data: dict[str, Any]) -> None:
    """Procesa una orden entrante del panel."""
    kind = (data.get("type") or "").lower()

    if kind == "control":
        action = (data.get("action") or "").lower()
        if action == "start":
            await hub.set_running(True)
        elif action == "pause":
            await hub.set_running(False)
        elif action == "attack":
            await hub.start_attack(int(data.get("size") or 16))
        elif action == "reset":
            await hub.reset()
        else:
            await hub.send_to(ws, {"type": "error", "data": {"detail": f"accion desconocida: {action}"}})
        return

    if kind == "config":
        changed = hub.update_config(
            similarity_threshold=data.get("similarity_threshold"),
            stream_speed=data.get("stream_speed"),
            frequency_window=data.get("frequency_window"),
            frequency_limit=data.get("frequency_limit"),
        )
        await hub.broadcast({"type": "config", "data": changed})
        return

    if kind == "manual":
        user = (data.get("user") or "invitado").strip()[:32] or "invitado"
        text = (data.get("text") or "").strip()
        if not text:
            await hub.send_to(ws, {"type": "error", "data": {"detail": "mensaje vacio"}})
            return
        await hub.publish(user, text[:500])
        return

    if kind == "block":
        user = (data.get("user") or "").strip()
        if user:
            await hub.block(user)
        return

    if kind == "unblock":
        user = (data.get("user") or "").strip()
        if user:
            await hub.unblock(user)
        return

    await hub.send_to(ws, {"type": "error", "data": {"detail": f"tipo desconocido: {kind}"}})


def _resolve_port() -> int:
    """Puerto de escucha desde $PORT (lo inyectan Render / Railway / Docker)."""
    raw = os.environ.get("PORT", "").strip()
    if raw.isdigit():
        return int(raw)
    return 8000


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",       # obligatorio en contenedores
        port=_resolve_port(),
        proxy_headers=True,   # necesario detras del proxy de Render
        forwarded_allow_ips="*",
        reload=os.environ.get("APP_ENV", "production") == "development",
    )
