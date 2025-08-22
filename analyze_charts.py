# analyze_charts.py
# Produce interpretable charts from the output CSVs created by the ingestion pipeline.
# Requirements: pandas, matplotlib

import os
import sys
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

OUTPUT_DIR = "."          # where your CSVs are (change if needed)
CHART_DIR = "charts"      # where PNGs will be saved

# Filenames created by your pipeline
UNIFIED = "unified_post_data.csv"
BEST_HOURS = "best_hours_by_platform.csv"
HEATMAP = "best_times_heatmap_by_platform.csv"

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def safe_read_csv(path):
    if not os.path.exists(path):
        print(f"[WARN] Missing file: {path}")
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[WARN] Could not read {path}: {e}")
        return None

def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def parse_date(df, col="Post Date (JST)"):
    if col in df.columns:
        df["_date"] = pd.to_datetime(df[col], errors="coerce")
    else:
        df["_date"] = pd.NaT
    return df

def savefig(path):
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()

def main():
    ensure_dir(CHART_DIR)

    unified = safe_read_csv(os.path.join(OUTPUT_DIR, UNIFIED))
    if unified is None:
        print("[ERROR] unified_post_data.csv not found. Run the ingestion script first.")
        sys.exit(1)

    # Basic prep
    unified = coerce_numeric(unified, [
        "Impressions", "Reach", "Engagements", "Engagement Rate",
        "Link Clicks", "Follows Gained (estimated)", "Time of Day (hour)"
    ])
    unified = parse_date(unified, "Post Date (JST)")

    # Fill platform missing
    if "Platform" not in unified.columns:
        unified["Platform"] = "Unknown"

    # 1) Posts over time by platform (count of posts/day)
    if "_date" in unified.columns:
        posts_per_day = (unified
                         .dropna(subset=["_date"])
                         .groupby(["Platform", "_date"], dropna=False)
                         .size()
                         .reset_index(name="Posts"))
        if not posts_per_day.empty:
            plt.figure(figsize=(10, 5))
            for plat, g in posts_per_day.groupby("Platform"):
                g = g.sort_values("_date")
                plt.plot(g["_date"], g["Posts"], label=plat)
            plt.title("Posts per Day by Platform")
            plt.xlabel("Date (JST)")
            plt.ylabel("Posts")
            plt.legend()
            savefig(os.path.join(CHART_DIR, "01_posts_per_day_by_platform.png"))

    # 2) Engagement Rate distribution (boxplot) per platform
    er = unified.dropna(subset=["Engagement Rate"])
    if not er.empty:
        plats = sorted(er["Platform"].dropna().unique().tolist())
        data = [er.loc[er["Platform"] == p, "Engagement Rate"].values for p in plats]
        plt.figure(figsize=(8, 5))
        plt.boxplot(data, labels=plats, showfliers=False)
        plt.title("Engagement Rate Distribution by Platform")
        plt.ylabel("Engagement Rate (%)")
        savefig(os.path.join(CHART_DIR, "02_engagement_rate_boxplot_by_platform.png"))

    # 3) Avg engagement rate by Format (per platform)
    if "Format" in unified.columns:
        fmt = (unified
               .groupby(["Platform", "Format"], dropna=False)["Engagement Rate"]
               .mean()
               .reset_index()
               .sort_values(["Platform", "Engagement Rate"], ascending=[True, False]))
        for plat, g in fmt.groupby("Platform"):
            if g["Format"].notna().any():
                order = g.sort_values("Engagement Rate", ascending=False)
                plt.figure(figsize=(10, 5))
                plt.bar(order["Format"].astype(str), order["Engagement Rate"])
                plt.xticks(rotation=30, ha="right")
                plt.title(f"Avg Engagement Rate by Format — {plat}")
                plt.ylabel("Engagement Rate (%)")
                plt.xlabel("Format")
                savefig(os.path.join(CHART_DIR, f"03_avg_engagement_rate_by_format_{plat}.png"))

    # 4) Day of Week average engagement rate (overall + per platform)
    if "Day of Week" in unified.columns:
        dow_overall = (unified
                       .groupby("Day of Week", dropna=False)["Engagement Rate"]
                       .mean().reset_index())
        if not dow_overall.empty:
            # Keep Mon..Sun ordering if your pipeline used categorical; otherwise sort manually
            cat_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            dow_overall["__rank"] = dow_overall["Day of Week"].apply(lambda x: cat_order.index(x) if x in cat_order else 999)
            dow_overall = dow_overall.sort_values("__rank")
            plt.figure(figsize=(8, 4))
            plt.bar(dow_overall["Day of Week"].astype(str), dow_overall["Engagement Rate"])
            plt.title("Avg Engagement Rate by Day of Week (Overall)")
            plt.ylabel("Engagement Rate (%)")
            plt.xlabel("Day of Week (JST)")
            savefig(os.path.join(CHART_DIR, "04_avg_engagement_rate_by_day_overall.png"))

        # Per platform
        for plat, g in unified.groupby("Platform"):
            gg = (g.groupby("Day of Week", dropna=False)["Engagement Rate"]
                  .mean().reset_index())
            if gg.empty: 
                continue
            gg["__rank"] = gg["Day of Week"].apply(lambda x: cat_order.index(x) if x in cat_order else 999)
            gg = gg.sort_values("__rank")
            plt.figure(figsize=(8, 4))
            plt.bar(gg["Day of Week"].astype(str), gg["Engagement Rate"])
            plt.title(f"Avg Engagement Rate by Day of Week — {plat}")
            plt.ylabel("Engagement Rate (%)")
            plt.xlabel("Day of Week (JST)")
            savefig(os.path.join(CHART_DIR, f"04_avg_engagement_rate_by_day_{plat}.png"))

    # 5) Best JST Hour by platform (bar from best_hours_by_platform.csv if present, else compute)
    best_hours = safe_read_csv(os.path.join(OUTPUT_DIR, BEST_HOURS))
    if best_hours is None:
        # compute fallback
        best_hours = (unified
                      .groupby(["Platform", "Time of Day (hour)"], dropna=False)["Engagement Rate"]
                      .median().reset_index().rename(columns={"Time of Day (hour)":"JST Hour",
                                                              "Engagement Rate":"med_eng_rate"}))
    if best_hours is not None and not best_hours.empty:
        for plat, g in best_hours.groupby("Platform"):
            gg = g.dropna(subset=["JST Hour"]).sort_values("med_eng_rate", ascending=False)
            plt.figure(figsize=(10, 4))
            plt.bar(gg["JST Hour"].astype(int).astype(str), gg["med_eng_rate"])
            plt.title(f"Median Engagement Rate by JST Hour — {plat}")
            plt.xlabel("Hour of Day (JST)")
            plt.ylabel("Median Engagement Rate (%)")
            savefig(os.path.join(CHART_DIR, f"05_median_eng_rate_by_hour_{plat}.png"))

    # 6) Day × JST Hour heatmaps per platform (median ER)
    # Prefer the precomputed heatmap CSV for stability
    heatmap_df = safe_read_csv(os.path.join(OUTPUT_DIR, HEATMAP))
    if heatmap_df is None:
        # Recompute from unified
        heatmap_list = []
        for plat, g in unified.groupby("Platform"):
            if "Day of Week" not in g.columns or "Time of Day (hour)" not in g.columns:
                continue
            pivot = g.pivot_table(index="Day of Week",
                                  columns="Time of Day (hour)",
                                  values="Engagement Rate",
                                  aggfunc="median")
            pivot["Platform"] = plat
            heatmap_list.append(pivot.reset_index())
        if heatmap_list:
            heatmap_df = pd.concat(heatmap_list, ignore_index=True)

    if heatmap_df is not None and not heatmap_df.empty:
        # columns are mixed: Day of Week, hour columns, Platform
        # Normalize day order
        cat_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        for plat, g in heatmap_df.groupby("Platform"):
            g = g.copy()
            if "Day of Week" not in g.columns:
                continue
            g["__rank"] = g["Day of Week"].apply(lambda x: cat_order.index(x) if x in cat_order else 999)
            g = g.sort_values("__rank")
            # Extract numeric hour columns
            hour_cols = [c for c in g.columns if c not in ("Day of Week","Platform","__rank")]
            # Some columns might be strings; try to sort by int where possible
            def to_int_or_nan(c):
                try:
                    return int(float(c))
                except Exception:
                    return np.nan
            hour_pairs = sorted([(c, to_int_or_nan(c)) for c in hour_cols], key=lambda t: (math.isnan(t[1]), t[1]))
            hour_cols_sorted = [p[0] for p in hour_pairs if not math.isnan(p[1])]

            vals = g[hour_cols_sorted].to_numpy(dtype=float)
            plt.figure(figsize=(12, 5))
            plt.imshow(vals, aspect="auto")
            plt.colorbar(label="Median Engagement Rate (%)")
            plt.title(f"Median Engagement Rate — Day × JST Hour — {plat}")
            plt.yticks(ticks=np.arange(len(g["Day of Week"])), labels=g["Day of Week"])
            plt.xticks(ticks=np.arange(len(hour_cols_sorted)), labels=[str(int(float(h))) for h in hour_cols_sorted], rotation=0)
            plt.xlabel("JST Hour")
            plt.ylabel("Day of Week")
            savefig(os.path.join(CHART_DIR, f"06_heatmap_day_hour_{plat}.png"))

            # Also export a ranked table for top slots
            long = g.melt(id_vars=["Day of Week"], value_vars=hour_cols_sorted, var_name="JST Hour", value_name="Median ER")
            long["Platform"] = plat
            long = long.dropna(subset=["Median ER"]).sort_values("Median ER", ascending=False)
            long.to_csv(os.path.join(CHART_DIR, f"06_top_slots_{plat}.csv"), index=False)

    # 7) Impressions vs Engagement Rate scatter (QC)
    qc = unified.dropna(subset=["Impressions", "Engagement Rate"])
    if not qc.empty:
        plt.figure(figsize=(7, 6))
        plt.scatter(qc["Impressions"], qc["Engagement Rate"], s=12, alpha=0.6)
        plt.xscale("log")  # impressions often skewed
        plt.title("Impressions vs Engagement Rate (All Platforms)")
        plt.xlabel("Impressions (log scale)")
        plt.ylabel("Engagement Rate (%)")
        savefig(os.path.join(CHART_DIR, "07_impressions_vs_engagement_rate_scatter.png"))

    # 8) Estimated followers gained per day per platform (sum/day)
    if "_date" in unified.columns and "Follows Gained (estimated)" in unified.columns:
        fg = (unified.dropna(subset=["_date"])
              .groupby(["Platform", "_date"], dropna=False)["Follows Gained (estimated)"]
              .sum().reset_index())
        if not fg.empty:
            for plat, g in fg.groupby("Platform"):
                g = g.sort_values("_date")
                plt.figure(figsize=(10, 4))
                plt.plot(g["_date"], g["Follows Gained (estimated)"])
                plt.title(f"Estimated Followers Gained per Day — {plat}")
                plt.xlabel("Date (JST)")
                plt.ylabel("Estimated Followers Gained")
                savefig(os.path.join(CHART_DIR, f"08_followers_gained_per_day_{plat}.png"))

    # 9) Quick table of top Day×Hour overall across platforms (median ER)
    if ("Day of Week" in unified.columns) and ("Time of Day (hour)" in unified.columns):
        grid = (unified.groupby(["Platform", "Day of Week", "Time of Day (hour)"], dropna=False)
                ["Engagement Rate"].median().reset_index().rename(columns={"Engagement Rate":"Median ER"}))
        if not grid.empty:
            top = (grid.dropna(subset=["Median ER"])
                   .sort_values(["Platform", "Median ER"], ascending=[True, False]))
            top.to_csv(os.path.join(CHART_DIR, "09_top_day_hour_overall.csv"), index=False)

    print(f"All charts saved to: {os.path.abspath(CHART_DIR)}")

if __name__ == "__main__":
    main()
