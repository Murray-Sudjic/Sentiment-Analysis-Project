# Sentiment Analysis Project

This project builds a pipeline to measure whether sentiment in Reddit posts and comments about energy/finance topics predicts ETF or stock returns.

## Project Structure
```
sentiment_project/
├─ config/
│  └─ scope_energy.yaml   # keywords, tickers, weights and basic info
├─ data/
│  ├─ raw/                # raw JSONL Reddit pulls (from ingest)
│  └─ work/               # All intermediate outputs
├─ src/
│  ├─ ingest.py           # pulls Reddit posts/comments
│  ├─ clean.py            # filters + text construction
│  ├─ features.py         # sentiment scoring + weighting + aggregation
│  ├─ market.py           # fetches prices, computes forward returns
│  ├─ evaluate.py         # joins features with returns, computes correlation
│  └─ utils.py            # shared helpers
│ 
└─ run.py                 # orchestrates the full pipeline
```
### Config (`config/scope_energy.yaml`)

**Controls the scope of analysis:**

- `name`: scope name (used in file paths)
- `subreddits`: list of subreddits to pull from
- `keywords`: terms to search for
- `tickers`: stock/ETF tickers to track
- `name_map`: map of company names → ticker
- `decay`: exponential decay rate + cap for weighting
- `subreddit_weights`: relative importance of subreddits
- `time_filter`: Reddit API window (`hour`, `day`, `week`, …)
- `max_posts_per_query`: limit per query
- `top_comments`: number of comments to attach per post

**Example:**

```yaml
name: scope_energy
subreddits:
  - investing
  - stocks
  - energy
keywords:
  - oil
  - natural gas
  - renewables
  - battery
tickers:
  - XOM
  - CVX
  - BP
  - SHEL
  - COP
```
## Workflow

**1.	Ingest**  
Pull posts + top comments from Reddit with ingest.py. Saves JSONL under data/raw/.  
**2.	Clean**  
Filter by dates, keywords, tickers; construct clean text fields (clean.py).  
**3.	Score**  
Apply VADER sentiment, compute weights for each post/comment (features.py).  
**4.	Aggregate**  
Collapse to daily features (mean sentiment, weighted mean sentiment, counts).  
**5.	Market Data**  
Download adjusted close prices with Yahoo Finance (market.py), compute forward 1-day returns.  
**6.	Evaluate**  
Join sentiment features with returns, compute correlation (evaluate.py).  
**7.	Run All**  
Use run.py to execute all steps in one go.  

## Running the Pipeline

**First, activate your virtual environment and install requirements:**

`pip install -r requirements.txt`

Run end-to-end:
```
python run.py \
  --raw_posts data/raw/scope_energy/posts_2025-01-01.jsonl \
  --ticker XLE \
  --start 2025-01-01 \
  --end 2025-01-31 \
  --workdir data/work
```
**Arguments**  
- `--raw_posts` : Path to raw JSONL file from ingest.py  
- `--ticker` : Market ticker to evaluate against (e.g., XLE, TSLA)  
- `--start`, `--end` : Date range (YYYY-MM-DD)  
- `--workdir` : Directory for outputs (default: data/work)  

**Outputs**

Written to `--workdir`:  
- `clean_posts.jsonl` : filtered posts + comments  
- `scored_posts.jsonl` : sentiment + weights per item  
- `features_daily.csv` : daily aggregated sentiment features  
- `returns_daily.csv` : forward returns from market prices  
- `joined_and_corr.csv` : features + returns joined, with correlation row  

Console will also print:

=== Evaluation ===  
sent_mean_weighted ret_fwd_1d  
Correlation  Correlation: 0.123  P-value: 0.45  

## Notes
	•	The pipeline includes both posts and comments; comment weights are scaled down relative to post size.
	•	Dates are UTC. Forward returns drop the final day (no next-day price).
	•	Currently all I/O is CSV/JSONL. Parquet support has been removed for simplicity.
	•	Config (scope_energy.yaml) controls tickers, keywords, weights — update this file to change scope.