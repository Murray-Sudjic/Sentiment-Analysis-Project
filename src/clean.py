import pandas as pd
from langdetect import detect
import json
import re
from typing import List
from langdetect.lang_detect_exception import LangDetectException

class Cleaner:
    def __init__(self, cfg: dict):
        self.tickers = set(cfg.get("tickers", []))
        self.name_map = {k.lower(): v for k, v in cfg.get("name_map", {}).items()}  # ensure key matches YAML
        self.sector_keys = [k.lower() for k in cfg.get("keywords", [])]

    def extract_tickers(self, text: str) -> List[str]:
        syms = set()
        for sym in re.findall(r"\b\$?([A-Z]{1,5})\b", text):
            if sym in self.tickers:
                syms.add(sym)           

        low = text.lower()
        for name, sym in self.name_map.items():
            if re.search(rf"\b{re.escape(name)}\b", low): # re.escape ensures that any regex characters are treated literally
                syms.add(sym)
        return sorted(syms) # returns sorted list

    def row_filtering(self, file_path: str, start_ts: int, end_ts: int):

        with open(file_path, "r") as fin:
            for line in fin:
                if not line.strip(): # checks if line is empty or just whitespace
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # Checks ordered cheap -> expensive
                created_utc = int(record.get("created_utc")) # seconds since Unix epoch
                if created_utc is None or not (start_ts <= created_utc <= end_ts): 
                    continue

                post_id_ok = bool(str(record.get("post_id","")).strip())
                if not post_id_ok:
                    continue

                title_ok = bool(str(record.get("title","")).strip())
                body_ok  = bool(str(record.get("selftext","")).strip())
                if not (title_ok or body_ok):
                    continue

                if record.get("score", 0) < 5: #checks has >= 5 upvotes
                    continue

                text = " ".join([record.get("title",""), record.get("selftext","")]).strip()
                if not text:
                    continue
                try:
                    if detect(text) != "en":
                        continue # skip non-english
                except LangDetectException:
                    continue # skip if detection fails
                
                yield record

    def text_construction(self, records, out_path):
        import json
        with open(out_path, "w") as fout:
            for rec in records:
                title = rec.get("title", "")
                selftext = rec.get("selftext", "")
                text_clean = self.regex_filtering((" ".join([title, selftext])).strip())
                if self.is_spam(text_clean):
                    continue

                rec["text_clean"] = text_clean
                rec["text_len_words"] = len(text_clean.split())
                rec["is_english"] = True
                low_text = text_clean.lower()
                syms = self.extract_tickers(title + " " + selftext)
                rec["tickers"] = syms
                rec["has_ticker"] = bool(syms)
                rec["sector_keyword_present"] = any(k in low_text for k in self.sector_keys)
                rec["in_scope"] = rec["has_ticker"] or rec["sector_keyword_present"]

                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def regex_filtering(self, clean_text: str):
        clean_text = re.sub(r"[\w\.-]+@[\w\.-]+\.\w+", r"<\g<0>>", clean_text) # puts <> around email addresses
        clean_text = re.sub(r"\s{2,}", " ", clean_text) # removes repeated spaces
        clean_text = re.sub(r"(https?://[^\s]+|www\.[^\s]+)", r"<\g<0>>", clean_text) # puts <> around URL
        clean_text = re.sub(r"^>.*$", "", clean_text, flags=re.MULTILINE) # Remove blockquotes at the start of a line (> something)
        clean_text = re.sub(r"```.*?```", "", clean_text, flags=re.DOTALL) # Remove code blocks wrapped in triple backticks (```...```)
        clean_text = re.sub(r"`[^`]+`", "", clean_text) # Remove inline code wrapped in single backticks (`...`)
        return clean_text

    def is_spam(self, clean_text: str) -> bool:

        spam_keywords = ["buy now", "free", "click here", "subscribe", "visit", "offer"]
        if any(word in clean_text.lower() for word in spam_keywords):
            return True
        if re.search(r"(https?://[^\s]+)", clean_text):  # only link
            if len(clean_text.split()) <= 3:
                return True
        if clean_text.count("!") > 5:
            return True
        return False
