import yaml
import praw
import json, os
from datetime import datetime, timezone
import pandas as pd

class IngestService:
    def __init__(self, reddit_client, config, repository):
        self.reddit = reddit_client         # PRAW client
        self.config = config                # YAML scope (subreddits, keywords, etc.)
        self.repo = repository              # handles file writing
        self.posts_df = None
        self.comments_df = None
        self.meta = {}

    def fetch_posts(self):
        rows = []
        num_posts = self.config.get("max_posts_per_query", 10)
        for sub in self.config['subreddits']:
            for keyword in self.config['keywords']:
                submissions = self.reddit.subreddit(sub).search(keyword, limit=num_posts, sort="new", time_filter=self.config['time_filter'])
                for s in submissions:
                    rows.append({
                        "post_id": s.id,
                        "subreddit": sub,
                        "created_utc": int(s.created_utc),
                        "title": s.title,
                        "selftext": s.selftext,
                        "score": s.score,
                        "num_comments": s.num_comments,
                        "url": s.url,
                        "keyword_matched": keyword,
                        "scope_name": self.config['name'],
                        "ingested_at_utc": int(datetime.now(timezone.utc).timestamp()),
                        "is_comment": False
                    })
        self.posts_df = pd.DataFrame(rows)


    def fetch_comments(self):
        #fetch top comments for each post.
        if not self.config.get("search_top_comments", False):
            self.comments_df = pd.DataFrame()
            return
        if self.posts_df is None or self.posts_df.empty:
            self.comments_df = pd.DataFrame()
            return

        rows = []
        for post_id in self.posts_df['post_id']:
            submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)
            for rank, c in enumerate(submission.comments[:self.config['top_comments']], start=1):  # top n, specified in yaml
                rows.append({
                    "post_id": post_id,
                    "comment_id": c.id,
                    "created_utc": int(c.created_utc),
                    "comment_text": c.body,
                    "comment_score": c.score,
                    "rank": rank,
                    "scope_name": self.config['name'],
                    "ingested_at_utc": int(datetime.now(timezone.utc).timestamp()),
                    "is_comment" : True
                })
        self.comments_df = pd.DataFrame(rows)

    def write_raw(self):
        """Hand off to repository to persist data."""
        today = datetime.date.today().strftime("%Y-%m-%d")

        self.meta = {
        "scope": self.config.get("name"),
        "time_filter": self.config.get("time_filter"),
        "subreddits": self.config.get("subreddits", []),
        "keywords": self.config.get("keywords", []),
        "posts_count": 0 if self.posts_df is None else len(self.posts_df),
        "comments_count": 0 if self.comments_df is None else len(self.comments_df),
        "ingested_at_utc": int(datetime.now(timezone.utc).timestamp())
        }
        self.repo.write_posts(self.posts_df, self.config, today)
        self.repo.write_comments(self.comments_df, self.config, today)
        self.repo.write_meta(self.meta, self.config, today)

class Repository:
    def __init__(self, base_dir="../data/raw"):
        self.base_dir = base_dir

    def write_posts(self, posts_df, config, today):
        outdir = os.path.join(self.base_dir, config['name'])
        os.makedirs(outdir, exist_ok=True)
        posts_df.to_json(f"{outdir}/posts_{today}.jsonl", orient="records", lines=True)

    def write_comments(self, comments_df, config, today):
        outdir = os.path.join(self.base_dir, config['name'])
        os.makedirs(outdir, exist_ok=True)
        comments_df.to_json(f"{outdir}/comments_{today}.jsonl", orient="records", lines=True)

    def write_meta(self, meta, config, today):
        outdir = os.path.join(self.base_dir, config['name'])
        os.makedirs(outdir, exist_ok=True)
        with open(f"{outdir}/meta_{today}.json", "w") as f:
            json.dump(meta, f, indent=2)