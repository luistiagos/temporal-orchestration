from datetime import UTC, datetime

from dsg_temporal.schedule import compute_due_at, parse_iso_datetime


def test_parse_iso_datetime_accepts_zulu():
    parsed = parse_iso_datetime("2026-05-11T12:00:00Z")
    assert parsed == datetime(2026, 5, 11, 12, 0, tzinfo=UTC)


def test_compute_due_at_uses_explicit_send_at():
    now = datetime(2026, 5, 11, 12, 0, tzinfo=UTC)
    due = compute_due_at(
        now=now,
        base_at=now,
        delay_minutes=60,
        send_at_iso="2026-05-12T10:00:00Z",
    )
    assert due == datetime(2026, 5, 12, 10, 0, tzinfo=UTC)


def test_compute_due_at_applies_delay():
    now = datetime(2026, 5, 11, 12, 0, tzinfo=UTC)
    due = compute_due_at(now=now, base_at=now, delay_minutes=30)
    assert due == datetime(2026, 5, 11, 12, 30, tzinfo=UTC)

