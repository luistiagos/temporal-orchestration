from __future__ import annotations

from datetime import UTC, datetime, timedelta


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_preferred_time(value: str | None) -> tuple[int, int, int] | None:
    if not value:
        return None
    parts = str(value).split(":")
    if len(parts) < 2:
        raise ValueError("preferred_time must be HH:MM or HH:MM:SS")
    hour = int(parts[0])
    minute = int(parts[1])
    second = int(parts[2]) if len(parts) > 2 else 0
    if hour < 0 or hour > 23 or minute < 0 or minute > 59 or second < 0 or second > 59:
        raise ValueError("preferred_time is out of range")
    return hour, minute, second


def compute_due_at(
    *,
    now: datetime,
    base_at: datetime | None,
    delay_minutes: int = 0,
    send_at_iso: str | None = None,
    preferred_time: str | None = None,
    preferred_day: int | None = None,
) -> datetime:
    explicit = parse_iso_datetime(send_at_iso)
    if explicit is not None:
        return explicit

    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    now = now.astimezone(UTC)

    base = base_at or now
    if base.tzinfo is None:
        base = base.replace(tzinfo=UTC)
    base = base.astimezone(UTC)
    if base < now:
        base = now

    due = base + timedelta(minutes=max(0, int(delay_minutes or 0)))
    preferred = parse_preferred_time(preferred_time)

    if preferred_day is not None:
        day = max(1, min(28, int(preferred_day)))
        due = due.replace(day=day)
        if due < base:
            month = due.month + 1
            year = due.year
            if month > 12:
                month = 1
                year += 1
            due = due.replace(year=year, month=month, day=day)

    if preferred is not None:
        hour, minute, second = preferred
        candidate = due.replace(hour=hour, minute=minute, second=second, microsecond=0)
        if candidate < due:
            candidate = candidate + timedelta(days=1)
        due = candidate

    return due

