from app import (
    CommentFeatures,
    MEDIUM_THRESHOLD,
    HIGH_THRESHOLD,
    RULES,
    build_features,
    score_comment,
    verdict_label,
    squash,
    RawComment,
)


def test_rules_sum_to_one():
    total = sum(rule.weight for rule in RULES)
    assert abs(total - 1.0) < 1e-9


def test_squash_bounds():
    assert squash(-1.0) == 0.0
    assert squash(0.08) >= 0.0
    assert squash(0.80) <= 1.0
    assert squash(10.0) == 1.0


def test_verdict_label_thresholds():
    assert verdict_label(HIGH_THRESHOLD) == "high"
    assert verdict_label(HIGH_THRESHOLD - 1e-9) == "medium"
    assert verdict_label(MEDIUM_THRESHOLD) == "medium"
    assert verdict_label(MEDIUM_THRESHOLD - 1e-9) == "low"


def test_score_comment_returns_verdict():
    features = CommentFeatures(
        comment_id="c1",
        author="usuario_normal",
        text="Me ha gustado mucho este video, gracias por la explicación",
        text_preview="Me ha gustado mucho este video, gracias por la explicación",
        published_at=None,
        depth=0,
        is_author=False,
    )
    verdict = score_comment(features)
    assert 0.0 <= verdict.bot_score <= 100.0
    assert verdict.features is features
    assert isinstance(verdict.reasons, list)
    assert isinstance(verdict.contributions, dict)
    assert len(verdict.contributions) == len(RULES)


def test_build_features_basic():
    comment = RawComment(
        comment_id="x1",
        author="test_user123",
        text="GANA DINERO DESDE CASA https://spam.example/x",
        published_at=None,
        like_count=0,
        reply_count=0,
        has_channel=False,
        depth=0,
    )
    f = build_features(comment)
    assert f.comment_id == "x1"
    assert f.link_count >= 1
    assert f.spam_score > 0.0
    assert isinstance(f.spam_terms, list)
