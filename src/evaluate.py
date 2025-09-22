import pandas as pd
from scipy.stats import pearsonr

def evaluate(features_path: str, returns_path: str, output_path: str):
    features = pd.read_csv(features_path, index_col=0, parse_dates=True)
    returns = pd.read_csv(returns_path, index_col=0, parse_dates=True)

    # Align on dates
    joined = features.join(returns, how="inner")

    # Keep only rows with valid data
    joined = joined.dropna(subset=["sent_mean_weighted", "ret_fwd_1d"])

    out_rows = [joined]

    if len(joined) >= 2:
        # Compute Pearson correlation
        corr, pval = pearsonr(joined["sent_mean_weighted"], joined["ret_fwd_1d"])

        # Add correlation result as a row
        corr_row = pd.DataFrame({
            "sent_mean_weighted": [f"Correlation: {corr:.6f}"],
            "ret_fwd_1d": [f"P-value: {pval:.6g}"]
        }, index=["Correlation"])
        out_rows.append(corr_row)
    else:
        # Not enough data for correlation
        warn_row = pd.DataFrame({
            "sent_mean_weighted": ["Not enough data"],
            "ret_fwd_1d": [f"Rows available: {len(joined)}"]
        }, index=["Correlation"])
        out_rows.append(warn_row)
        print(f"[warn] Not enough overlapping rows for correlation (got {len(joined)}).")

    # Save output
    out_df = pd.concat(out_rows)
    out_df.to_csv(output_path)