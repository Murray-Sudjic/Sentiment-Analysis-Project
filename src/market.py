import yfinance as yf
import pandas as pd

def fetch_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Download daily prices with yfinance and return a DataFrame
    with 'adj_close' as the main column.
    """
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if df.empty:
        raise ValueError(f"No data returned for {ticker}")
    df = df.rename(columns=str.lower)
    if "adj close" in df.columns:
        df["adj_close"] = df["adj close"]
        df = df[["adj_close"]]
        df.index = pd.to_datetime(df.index).normalize()  # 00:00:00, tz-naive
        df.index.name = "date"
        return df

def make_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Compute forward 1-day returns: r_{t+1} = (P_{t+1}/P_t - 1).
    """
    s = prices["adj_close"] # Pulls out the adj_close column as a Series s.

    fwd = s.shift(-1) / s - 1.0 # e.g., Today’s adj close = 100, tomorrow’s = 105 -> return = 105/100 - 1 = 0.05 (5%).
    
    # Builds a new DataFrame with two columns:
    #   adj_close: the price at time t
    #   ret_fwd_1d: the forward 1-day return from t → t+1
    out = pd.DataFrame({"adj_close": s, "ret_fwd_1d": fwd}).dropna()
    out.index = pd.to_datetime(out.index).normalize() # ensure same normalisation
    out.index.name = "date"
    return out