
import os
import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime

# -------------------------
# Config
# -------------------------
INPUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "."
OUTPUT_DIR = sys.argv[2] if len(sys.argv) > 2 else os.path.join(INPUT_DIR, "charts_out")
os.makedirs(OUTPUT_DIR, exist_ok=True)
PDF_PATH = os.path.join(OUTPUT_DIR, "RIZIN_SNS_Analysis_Report.pdf")

# Expected input files (produced by your ETL script)
FILES = {
    "unified": "unified_post_data.csv",
    "by_format": "summary_by_format.csv",
    "by_dow": "summary_by_dayofweek.csv",
    "by_theme": "summary_by_theme.csv",
    "by_hour_optional": "summary_by_hour.csv",  # optional
}

def load_csv(name):
    path = os.path.join(INPUT_DIR, FILES[name])
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)

def to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def short_label(url, maxlen=28):
    if not isinstance(url, str) or not url:
        return ""
    # prefer the last path segment or last 10 chars
    base = url.split("/")[-1]
    if len(base) < 8:
        base = url[-12:]
    return (base[:maxlen] + "…") if len(base) > maxlen else base

def safe_save(fig, fname, pdf=None):
    out_path = os.path.join(OUTPUT_DIR, fname)
    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    if pdf is not None:
        pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

def main():
    df_unified = load_csv("unified")
    df_fmt = load_csv("by_format")
    df_dow = load_csv("by_dow")
    df_theme = load_csv("by_theme")
    df_hour = load_csv("by_hour_optional")

    if df_unified is None:
        print("ERROR: unified_post_data.csv not found in", INPUT_DIR)
        sys.exit(1)

    # Prepare numeric conversions
    df_unified = to_numeric(df_unified, ["Impressions","Reach","Engagements","Engagement Rate","Link Clicks","Follows Gained (estimated)"])
    if df_fmt is not None:
        df_fmt = to_numeric(df_fmt, ["Avg Impressions","Avg Reach","Avg Engagements","Avg Engagement Rate","Avg Link Clicks","Avg Follows Gained"])
    if df_dow is not None:
        df_dow = to_numeric(df_dow, ["Avg Impressions","Avg Reach","Avg Engagements","Avg Engagement Rate","Avg Link Clicks","Avg Follows Gained"])
    if df_theme is not None:
        df_theme = to_numeric(df_theme, ["Avg Impressions","Avg Reach","Avg Engagements","Avg Engagement Rate","Avg Link Clicks","Avg Follows Gained"])
    if df_hour is not None:
        df_hour = to_numeric(df_hour, ["avg_impr","avg_er","avg_eng","follows"])

    # Order day of week
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    if df_dow is not None and "Day of Week" in df_dow.columns:
        df_dow["Day of Week"] = pd.Categorical(df_dow["Day of Week"], categories=dow_order, ordered=True)
        df_dow = df_dow.sort_values("Day of Week")

    # Open a multipage PDF
    with PdfPages(PDF_PATH) as pdf:
        # Page 1: Executive snapshot text
        total_posts = len(df_unified)
        by_platform = df_unified["Platform"].value_counts(dropna=False).to_dict()
        by_event = df_unified["Event"].value_counts(dropna=False).to_dict()
        date_min = pd.to_datetime(df_unified["Post Date (JST)"], errors="coerce").min()
        date_max = pd.to_datetime(df_unified["Post Date (JST)"], errors="coerce").max()
        total_follows = int(df_unified["Follows Gained (estimated)"].fillna(0).sum())

        summary_lines = [
            "RIZIN Social Analytics Snapshot",
            f"Posts analyzed: {total_posts}  |  Date range: {str(date_min.date())} – {str(date_max.date())}",
            f"Platform counts: {by_platform}",
            f"Event counts: {by_event}",
            f"Estimated follows attributed: {total_follows}",
            "",
            "Key takeaways to look for in charts:",
            "• Which formats have higher average engagement rates and total volume",
            "• Best posting days/hours (JST) by engagement rate",
            "• Which formats/platforms are driving estimated follows",
            "• Top posts by impressions and engagements",
        ]
        fig = plt.figure(figsize=(11, 8.5))
        plt.axis("off")
        plt.title("Executive Summary", loc="left")
        plt.text(0.02, 0.95, "\n".join(summary_lines), va="top", family="monospace")
        safe_save(fig, "00_summary_text.png", pdf)

        # Page 2: Avg ER by Format
        if df_fmt is not None and not df_fmt.empty:
            fig = plt.figure(figsize=(10,6))
            x = df_fmt["Format"]
            y = df_fmt["Avg Engagement Rate"]
            plt.bar(x, y)
            plt.ylabel("Avg Engagement Rate (%)")
            plt.title("Average Engagement Rate by Format")
            plt.xticks(rotation=0)
            safe_save(fig, "01_avg_er_by_format.png", pdf)

        # Page 3: Avg Impressions by Format
        if df_fmt is not None and not df_fmt.empty:
            fig = plt.figure(figsize=(10,6))
            plt.bar(df_fmt["Format"], df_fmt["Avg Impressions"])
            plt.ylabel("Avg Impressions")
            plt.title("Average Impressions by Format")
            safe_save(fig, "02_avg_impr_by_format.png", pdf)

        # Page 4: Estimated Follows by Format (sum from unified)
        fmt_follow = (df_unified.groupby("Format")["Follows Gained (estimated)"]
                      .sum().sort_values(ascending=False))
        if not fmt_follow.empty:
            fig = plt.figure(figsize=(10,6))
            plt.bar(fmt_follow.index.astype(str), fmt_follow.values)
            plt.ylabel("Estimated Follows (sum)")
            plt.title("Estimated Follows by Format")
            safe_save(fig, "03_follows_by_format.png", pdf)

        # Page 5: Avg ER by Day of Week
        if df_dow is not None and not df_dow.empty:
            fig = plt.figure(figsize=(10,6))
            plt.bar(df_dow["Day of Week"].astype(str), df_dow["Avg Engagement Rate"])
            plt.ylabel("Avg Engagement Rate (%)")
            plt.title("Average Engagement Rate by Day of Week (JST)")
            plt.xticks(rotation=45, ha="right")
            safe_save(fig, "04_avg_er_by_dow.png", pdf)

        # Page 6: Avg Impressions by Day of Week
        if df_dow is not None and not df_dow.empty:
            fig = plt.figure(figsize=(10,6))
            plt.bar(df_dow["Day of Week"].astype(str), df_dow["Avg Impressions"])
            plt.ylabel("Avg Impressions")
            plt.title("Average Impressions by Day of Week (JST)")
            plt.xticks(rotation=45, ha="right")
            safe_save(fig, "05_avg_impr_by_dow.png", pdf)

        # Page 7: Avg ER by Hour (if provided)
        if df_hour is not None and not df_hour.empty:
            df_hour = df_hour.sort_values("Time of Day (hour)") if "Time of Day (hour)" in df_hour.columns else df_hour.sort_values("Time of Day")
            hour_col = "Time of Day (hour)" if "Time of Day (hour)" in df_hour.columns else "Time of Day"
            er_col = "avg_er" if "avg_er" in df_hour.columns else "Avg Engagement Rate"
            fig = plt.figure(figsize=(10,6))
            plt.bar(df_hour[hour_col].astype(str), df_hour[er_col])
            plt.ylabel("Avg Engagement Rate (%)")
            plt.title("Average Engagement Rate by Hour (JST)")
            plt.xticks(rotation=0)
            safe_save(fig, "06_avg_er_by_hour.png", pdf)

        # Page 8: Estimated Follows by Platform
        plat_follow = (df_unified.groupby("Platform")["Follows Gained (estimated)"]
                       .sum().sort_values(ascending=False))
        if not plat_follow.empty:
            fig = plt.figure(figsize=(8,5))
            plt.bar(plat_follow.index.astype(str), plat_follow.values)
            plt.ylabel("Estimated Follows (sum)")
            plt.title("Estimated Follows by Platform")
            safe_save(fig, "07_follows_by_platform.png", pdf)

        # Page 9: Top 10 posts by Impressions (horizontal bars)
        df_top_impr = df_unified.sort_values("Impressions", ascending=False).head(10).copy()
        if not df_top_impr.empty:
            df_top_impr["Label"] = df_top_impr.apply(
                lambda r: f"{r['Platform']}|{r['Event'].replace('Rizin ','R ')}|{str(r['Post Date (JST)'])[:10]}|{short_label(str(r['Post URL']))}",
                axis=1
            )
            fig = plt.figure(figsize=(10,6))
            plt.barh(df_top_impr["Label"], df_top_impr["Impressions"])
            plt.xlabel("Impressions")
            plt.title("Top 10 Posts by Impressions")
            plt.gca().invert_yaxis()
            safe_save(fig, "08_top10_impressions.png", pdf)

        # Page 10: Top 10 posts by Engagements (horizontal bars)
        df_top_eng = df_unified.sort_values("Engagements", ascending=False).head(10).copy()
        if not df_top_eng.empty:
            df_top_eng["Label"] = df_top_eng.apply(
                lambda r: f"{r['Platform']}|{r['Event'].replace('Rizin ','R ')}|{str(r['Post Date (JST)'])[:10]}|{short_label(str(r['Post URL']))}",
                axis=1
            )
            fig = plt.figure(figsize=(10,6))
            plt.barh(df_top_eng["Label"], df_top_eng["Engagements"])
            plt.xlabel("Engagements")
            plt.title("Top 10 Posts by Engagements")
            plt.gca().invert_yaxis()
            safe_save(fig, "09_top10_engagements.png", pdf)

        # Page 11: Impressions vs ER scatter (each dot = post)
        if "Engagement Rate" in df_unified.columns and "Impressions" in df_unified.columns:
            fig = plt.figure(figsize=(8,6))
            plt.scatter(df_unified["Impressions"], df_unified["Engagement Rate"])
            plt.xlabel("Impressions")
            plt.ylabel("Engagement Rate (%)")
            plt.title("Post Scatter: Impressions vs Engagement Rate")
            safe_save(fig, "10_scatter_impr_vs_er.png", pdf)

    print("Saved charts to:", OUTPUT_DIR)
    print("PDF report:", PDF_PATH)

if __name__ == "__main__":
    main()
