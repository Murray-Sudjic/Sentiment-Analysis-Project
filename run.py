import argparse
import os
import yaml

from src.clean import Cleaner
from src.features import FeatureProcessor
from src.market import fetch_prices, make_forward_returns
from src.evaluate import evaluate
from src.utils import to_epoch_seconds
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="End-to-end runner: clean -> score -> aggregate -> returns -> evaluate")
    ap.add_argument("--raw_posts", required=True, help="Path to raw posts JSONL (from ingest)")
    ap.add_argument("--ticker", required=True, help="ETF/stock ticker for returns (e.g., XLE)")
    ap.add_argument("--start", required=True, help="Start date (YYYY-MM-DD) for cleaning/returns")
    ap.add_argument("--end", required=True, help="End date (YYYY-MM-DD) for cleaning/returns")
    ap.add_argument("--workdir", default="data/work", help="Directory to write intermediate outputs")
    args = ap.parse_args()

    os.makedirs(args.workdir, exist_ok=True)

    clean_out = os.path.join(args.workdir, "clean_posts.jsonl")
    scored_out = os.path.join(args.workdir, "scored_posts.jsonl")
    features_out = os.path.join(args.workdir, "features_daily.csv")
    returns_out = os.path.join(args.workdir, "returns_daily.csv")
    eval_out = os.path.join(args.workdir, "joined_and_corr.csv")

    start_ts = to_epoch_seconds(args.start)
    end_ts = to_epoch_seconds(args.end) + (24 * 3600 - 1)

    with open("config/scope_energy.yaml") as f:
        cfg = yaml.safe_load(f)

    cleaner = Cleaner(cfg)
    feats = FeatureProcessor(cfg)

    records = cleaner.row_filtering(args.raw_posts, start_ts, end_ts)
    cleaner.text_construction(records, clean_out)

    # Only call once
    feats.process_file(clean_out, scored_out)
    feats.aggregate_daily(scored_out, features_out)

    prices = fetch_prices(args.ticker, args.start, args.end)
    rets = make_forward_returns(prices)
    rets.to_csv(returns_out)

    evaluate(features_out, returns_out, eval_out)

    joined = pd.read_csv(eval_out, index_col=0, parse_dates=True)
    corr_row = joined.tail(1)
    print("\n=== Evaluation ===")
    print(corr_row)

if __name__ == "__main__":
    main()