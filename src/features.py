from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from typing import Dict
import json
import math

class FeatureProcessor:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.analyser = SentimentIntensityAnalyzer()
        self.decay_values = cfg.get("decay", {})
        self.subreddit_weights = cfg.get("subreddit_weights", {})
        self.lmbda = float(self.decay_values.get("lambda", 0.0))
        self.cap = float(self.decay_values.get("cap", 10.0))

    def get_scores(self, record: Dict) -> Dict[str, float]:
        return self.analyser.polarity_scores(record["text_clean"])

    def attach_scores(self, record: Dict) -> Dict:
        scores = self.get_scores(record)
        record.update(scores)
        # Attach source type for downstream aggregation
        record["source_type"] = "comment" if record.get("is_comment") else "post"
        # Precompute a simple engagement weight
        record["weight"] = self.compute_weight(record)
        return record

    def compute_weight(self, record: Dict) -> float:
        """
        Computes a weight for each row in jsonl
        Posts: use score & num_comments.
        Comments: use comment_score and rank (1=top).
        Falls back to 1.0 if fields are missing.
        """
        age_hours = max(0.0,(float(record.get("ingested_at_utc", 0)) - float(record.get("created_utc", 0))) / 3600.0) # Seconds -> Hours
        is_comment = bool(record.get("is_comment", False))
        entity_boost = 1.0 + 0.2 * len(record.get("tickers", [])) + (0.2 if record.get("sector_keyword_present", False) else 0.0)

        if is_comment:
            cs = max(0, int(record.get("comment_score", 0)))
            rank = int(record.get("rank", 1)) if record.get("rank") else 1
            # Higher rank (1) gets more weight; ensure positive
            rank_factor = 1.0 / max(1, rank) # <=1
            base = 1 + math.log1p(cs) # weight grows with comment_score but log dampens big values; rank scales top comments higher
            decay = math.exp(-self.lmbda * age_hours)
            total_comments = int(record.get("num_comments", 0))
            scale = 1.0 / (1 + max(0, total_comments)) # scales comments down so not to dominate posts when mean is calculated
            comment_weight = min(self.cap, base * rank_factor * decay * entity_boost * scale)
            return comment_weight
        
        else:
            sc = max(0, int(record.get("score", 0)))
            nc = max(0, int(record.get("num_comments", 0)))
            base = 1.0 + math.log1p(sc) + 0.5 * math.log1p(nc) # base score, diminishing returns
            decay = math.exp(-self.lmbda * age_hours) # e.g., λ = 0.02 → ~20% decay per 10 hours
            sub_w = float(self.subreddit_weights.get(record.get("subreddit"), 1.0))  # default 1.0 if missing
            quality = 0.2 if bool(record.get("is_spam", False)) else 1.0 # reduces score if detected as spam
            short_penalty = 0.5 if int(record.get("text_len_words", 0)) < 5 else 1.0 # reduces score for short posts
            post_weight = min(self.cap, base * entity_boost * decay * sub_w * quality * short_penalty)
            return post_weight

    def process_file(self, in_path: str, out_path: str) -> None:
        """
        Read a JSONL file of cleaned records - must contain 'text_clean'
        Attach VADER sentiment scores to each, and write JSONL to out_path
        """
        with open(in_path, "r") as fin, open(out_path, "w") as fout:
            for line in fin:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # skip if no text_clean
                if "text_clean" not in rec or not isinstance(rec["text_clean"], str) or not rec["text_clean"].strip():
                    continue
                rec = self.attach_scores(rec)
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def aggregate_daily(self, scored_path: str, features_out: str) -> None:
        """
        Read scored JSONL, filter, group by calendar date (UTC),
        compute weighted & plain mean sentiment for posts and 
        comments that day.
        """
        import pandas as pd

        df = pd.read_json(scored_path, lines=True)
        if "in_scope" in df.columns:
            df = df[df["in_scope"] == True]

        needed = {"compound", "weight", "created_utc"}
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in scored data: {missing}")

        dt_utc = pd.to_datetime(df["created_utc"], unit="s", utc=True)
        df["date"] = dt_utc.dt.normalize().dt.tz_localize(None)  # 00:00:00, tz-naive
        
        grp = df.groupby("date", as_index=True) # creates groups of dates, and applies following lambda function to each group
        sent_mean_weighted = grp.apply(lambda x: 0.0 if x["weight"].sum() == 0 
                                       else (x["compound"] * x["weight"]).sum() / x["weight"].sum(),
                                       include_groups=False
                                       )
        sent_mean = grp["compound"].mean()
        n_items = grp.size()

        out = (
            pd.DataFrame(
                {
                    "sent_mean_weighted": sent_mean_weighted,
                    "sent_mean": sent_mean,
                    "n_items": n_items,
                }
            ).sort_index()
        )
        out.index.name = "date"
        out.to_csv(features_out, index=True)