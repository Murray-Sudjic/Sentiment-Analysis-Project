from datetime import datetime, timezone

def to_epoch_seconds(datestr: str) -> int:
    """YYYY-MM-DD -> epoch seconds at 00:00:00 UTC"""
    dt = datetime.strptime(datestr, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())