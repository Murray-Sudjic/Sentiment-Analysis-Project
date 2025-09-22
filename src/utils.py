from datetime import datetime, timezone

def to_epoch_seconds(datestr: str) -> int:
    """YYYY-MM-DD -> epoch seconds at 00:00:00 UTC"""
    dt = datetime.strptime(datestr, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def combine_jsonl(inputs, out_path):
    with open(out_path, "w", encoding="utf-8") as out:
        for path in inputs:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    out.write(line)
    return out_path