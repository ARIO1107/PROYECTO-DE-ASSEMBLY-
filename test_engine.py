"""Tests del motor heuristico de SocialBotGuard.

Cubren las tres reglas, la similitud de SequenceMatcher + Jaccard, la frecuencia
por ventana deslizante con penalizacion progresiva y alerta de spam, la
configuracion en vivo y la lista negra. Usan tiempos controlados (sin sleeps),
asi que son deterministas.

Ejecutar:  pytest -q
"""

import asyncio

from app import (
    BotEngine,
    ModerationHub,
    ShuffleBag,
    Verdict,
    count_urls,
    is_uppercase_blast,
    jaccard_similarity,
    similarity_score,
    text_similarity,
    BOT_THRESHOLD,
    FREQUENCY_RISK,
    FREQUENCY_STEP,
    FREQUENCY_MAX_RISK,
    PATTERN_RISK,
    RANGES,
    SIMILARITY_RISK,
)


DISTINCT = [
    "el clima hoy esta muy agradable en la ciudad",
    "quiero aprender a cocinar pasta italiana",
    "ayer vi una pelicula de ciencia ficcion",
    "necesito comprar una bicicleta nueva",
    "mi gato duerme todo el dia en el sofa",
    "estoy leyendo un libro sobre historia antigua",
]


# --------------------------- helpers puros ---------------------------
def test_count_urls():
    assert count_urls("sin enlaces") == 0
    assert count_urls("mira https://uno.com") == 1
    assert count_urls("a https://uno.com b http://dos.net c") == 2


def test_is_uppercase_blast():
    assert is_uppercase_blast("ESTO ESTA TODO EN MAYUSCULAS") is True
    assert is_uppercase_blast("esto esta en minusculas normales") is False
    assert is_uppercase_blast("JAJA") is False  # demasiado corto


def test_text_similarity():
    assert text_similarity("hola mundo", "hola mundo") == 1.0
    assert text_similarity("abc", "xyz") < 0.2
    assert text_similarity("", "algo") == 0.0


def test_jaccard_similarity():
    assert jaccard_similarity("gana dinero rapido ya", "gana dinero rapido ya") == 1.0
    assert jaccard_similarity("uno dos tres", "cuatro cinco seis") == 0.0
    s = jaccard_similarity("Gana 500$ diarios aqui", "Gana 500 USD diarios aqui")
    assert 0.0 < s < 1.0


def test_similarity_score_combines_both():
    score, seq, jac = similarity_score("hola mundo feliz", "hola mundo feliz")
    assert score == 1.0 and seq == 1.0 and jac == 1.0
    # Una variacion con reordenacion sube el Jaccard aunque el Seq baje.
    score2, seq2, jac2 = similarity_score(
        "gana dinero rapido bit.ly/promo", "rapido dinero gana bit.ly/promo"
    )
    assert score2 == max(seq2, jac2) and jac2 > 0.5


def test_shuffle_bag_no_repeat_until_exhausted():
    bag = ShuffleBag(["a", "b", "c", "d"])
    first = [bag.next() for _ in range(4)]
    assert sorted(first) == ["a", "b", "c", "d"]  # sin repetir hasta agotar


# --------------------------- regla 1: frecuencia progresiva ---------------------------
def test_frequency_progressive_and_alert():
    engine = BotEngine(frequency_window=5.0, frequency_limit=3)
    base = 1000.0
    results = [engine.analyze("u", t, now=base + i * 0.5) for i, t in enumerate(DISTINCT)]

    assert "frecuencia" not in results[2].matched          # 3 mensajes: aun no
    v4, v5, v6 = results[3], results[4], results[5]        # 4, 5 y 6 mensajes
    assert "frecuencia" in v4.matched
    assert v4.risk == FREQUENCY_RISK                        # 40 base
    assert v4.spam_alert is False
    assert v5.risk == FREQUENCY_RISK + FREQUENCY_STEP       # 50 progresivo
    assert v5.spam_alert is True                            # alerta roja automatica
    assert v6.risk == min(FREQUENCY_RISK + 2 * FREQUENCY_STEP, FREQUENCY_MAX_RISK)  # 60 (tope)


def test_frequency_window_expires():
    engine = BotEngine(frequency_window=5.0, frequency_limit=3)
    for i, t in enumerate(DISTINCT):
        v = engine.analyze("u", t, now=1000 + i * 10)       # espaciados > 5 s
    assert "frecuencia" not in v.matched


# --------------------------- regla 2: similitud ---------------------------
def test_similarity_exact_repeat():
    engine = BotEngine()
    texto = "compra seguidores baratos en este enlace"
    engine.analyze("u", texto, now=1000)
    v = engine.analyze("u", texto, now=1010)
    assert "similitud" in v.matched
    assert v.risk == SIMILARITY_RISK
    assert v.similarity == 1.0


def test_similarity_across_users():
    engine = BotEngine()
    texto = "visita mi perfil para mas informacion"
    engine.analyze("bot_a", texto, now=1000)
    v = engine.analyze("bot_b", texto, now=1001)
    assert "similitud" in v.matched


def test_similarity_threshold_is_configurable():
    a = "Gana 500$ diarios aqui bit.ly/promo"
    b = "Gana 500 USD diarios en bit.ly/promo"

    loose = BotEngine(similarity_threshold=0.50)
    loose.analyze("u", a, now=0)
    assert "similitud" in loose.analyze("u", b, now=1).matched

    strict = BotEngine(similarity_threshold=0.99)
    strict.analyze("u", a, now=0)
    assert "similitud" not in strict.analyze("u", b, now=1).matched


# --------------------------- regla 3: patrones ---------------------------
def test_patterns_multiple_urls():
    engine = BotEngine()
    v = engine.analyze("u", "mira https://uno.com y http://dos.net ahora", now=1000)
    assert "patrones" in v.matched and v.risk == PATTERN_RISK


def test_patterns_uppercase():
    engine = BotEngine()
    v = engine.analyze("u", "ESTO ES SPAM EN MAYUSCULAS", now=1000)
    assert "patrones" in v.matched and v.risk == PATTERN_RISK


# --------------------------- combinaciones y umbral ---------------------------
def test_similarity_plus_patterns_reaches_threshold():
    engine = BotEngine()
    texto = "GANATE SEGUIDORES GRATIS AHORA MISMO"
    engine.analyze("u", texto, now=2000)
    v = engine.analyze("u", texto, now=2500)
    assert v.risk == SIMILARITY_RISK + PATTERN_RISK
    assert v.risk >= BOT_THRESHOLD and v.is_bot is True
    assert isinstance(v, Verdict) and v.reason


def test_risk_is_capped_at_100():
    engine = BotEngine()
    t = "COMPRA YA https://a.com https://b.com"
    engine.analyze("u", t, now=4000)
    engine.analyze("u", t, now=4000.1)
    engine.analyze("u", t, now=4000.2)
    v = engine.analyze("u", t, now=4000.3)
    assert v.risk == 100


# --------------------------- hub: contadores, config y blacklist ---------------------------
def test_hub_counts_and_bot_detection():
    hub = ModerationHub()

    async def run():
        await hub.publish("humano", "hola, buen video")
        await hub.publish("promo", "GANATE SEGUIDORES GRATIS AHORA")
        await hub.publish("promo", "GANATE SEGUIDORES GRATIS AHORA")

    asyncio.run(run())
    stats = hub.stats()
    assert stats["total"] == 3
    assert stats["bots"] >= 1
    assert stats["real"] == 3 - stats["bots"]


def test_hub_block_and_unblock():
    hub = ModerationHub()

    async def run():
        await hub.publish("bot", "spam uno distinto aqui")
        await hub.block("bot")
        blocked = (hub.stats()["blocked"], list(hub.stats()["blocked_users"]))
        stopped = await hub.publish("bot", "otro mensaje cualquiera")  # bloqueado: None
        await hub.unblock("bot")
        return blocked, stopped

    (blocked_count, blocked_users), stopped = asyncio.run(run())
    assert blocked_count == 1 and blocked_users == ["bot"]
    assert stopped is None
    stats = hub.stats()
    assert stats["blocked"] == 0 and stats["blocked_users"] == []


def test_hub_config_clamps_and_propagates():
    hub = ModerationHub()
    cfg = hub.update_config(similarity_threshold=1.5, stream_speed=0.01)
    assert cfg["similarity_threshold"] == RANGES["similarity_threshold"][1]   # 0.90
    assert cfg["stream_speed"] == RANGES["stream_speed"][0]                   # 0.25
    assert hub.engine.similarity_threshold == cfg["similarity_threshold"]
    hub.update_config(similarity_threshold=0.7, frequency_limit=99)
    assert hub.config["similarity_threshold"] == 0.7
    assert hub.config["frequency_limit"] == RANGES["frequency_limit"][1]      # 10
    assert hub.engine.frequency_limit == 10


def test_hub_reset_keeps_config():
    hub = ModerationHub()

    async def run():
        hub.update_config(similarity_threshold=0.7, stream_speed=2.0)
        await hub.publish("humano", "mensaje de prueba largo")
        await hub.reset()

    asyncio.run(run())
    stats = hub.stats()
    assert stats["total"] == 0 and stats["bots"] == 0 and stats["blocked"] == 0
    assert len(hub.messages) == 0
    assert hub.config["similarity_threshold"] == 0.7
    assert hub.engine.similarity_threshold == 0.7
