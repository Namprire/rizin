import os
import re
import logging
from collections import defaultdict
from datetime import datetime

import fitz  # PyMuPDF
import pandas as pd
from zoneinfo import ZoneInfo

# -----------------------
# Config
# -----------------------
base_dir = "."  # adjust if needed
JST = ZoneInfo("Asia/Tokyo")
# If your CSV timestamps are UTC, keep UTC here.
# If they are already JST, set to ZoneInfo("Asia/Tokyo").
DEFAULT_SOURCE_TZ = ZoneInfo("UTC")

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

DOW_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# -----------------------
# Dictionaries
# -----------------------
event_keywords = {
    "korea": "Rizin South Korea",
    "ws_korea": "Rizin South Korea",
    "landmark": "Rizin Landmark",
    "rizin 4": "Rizin 4",
    "rizin4": "Rizin 4",
}

theme_dict = {
    "fightorder": "FightOrder",
    "behindthescenes": "BehindTheScenes",
}

# -----------------------
# Helpers
# -----------------------
def parse_to_jst_any(x):
    """
    Parse a wide range of timestamp inputs:
    - str: many common formats, ISO8601 (with/without Z/offset)
    - int/float: epoch seconds or milliseconds
    Localize naive times to DEFAULT_SOURCE_TZ, then convert to JST.
    Returns (jst_date_str 'MM/DD/YYYY', jst_hour_int 0-23, jst_dow_str) or (None, None, None).
    """
    # epoch numbers
    if isinstance(x, (int, float)) and not pd.isna(x):
        # guess ms vs s
        val = float(x)
        unit = "ms" if val > 1e11 else "s"
        try:
            dt = pd.to_datetime(val, unit=unit, utc=False)
        except Exception:
            return None, None, None
    else:
        # strings or pandas timestamps
        if isinstance(x, pd.Timestamp):
            dt = x
        else:
            xs = str(x).strip()
            if not xs or xs.lower() in {"nan", "nat"}:
                return None, None, None
            # Let pandas try best-effort parse (will keep tz if present)
            dt = pd.to_datetime(xs, errors="coerce", utc=False)
            if pd.isna(dt):
                # Try a few strict formats you used before
                for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
                    try:
                        naive = datetime.strptime(xs, fmt)
                        dt = pd.Timestamp(naive)
                        break
                    except Exception:
                        continue
                if pd.isna(dt):
                    return None, None, None

    # Localize if naive
    if dt.tzinfo is None:
        dt = dt.tz_localize(DEFAULT_SOURCE_TZ)

    jst = dt.tz_convert(JST)
    return jst.strftime("%m/%d/%Y"), int(jst.hour), jst.strftime("%A")


DATE_CANDIDATES = [
    "date", "datetime", "date_time", "post_date", "posted_at", "posted time", "sent at",
    "created_at", "created time", "created_time",
    "published_at", "published time", "published_time",
    "time", "time (utc)", "timestamp"
]

def extract_timestamp_from_row(row, df_cols_lower):
    """
    Try multiple column names; also handle separate 'date' + 'time' columns.
    Returns a value suitable for parse_to_jst_any (string / number / Timestamp) or None.
    """
    # try combined candidates first
    for key in DATE_CANDIDATES:
        if key in df_cols_lower:
            val = row.get(df_cols_lower[key], None)
            if pd.notna(val) and str(val).strip():
                return val

    # separate date + time columns (various spellings)
    date_keys = [k for k in df_cols_lower if "date" in k and "update" not in k]
    time_keys = [k for k in df_cols_lower if re.search(r"\btime\b", k) and "update" not in k]
    # prefer simple 'date' and 'time'
    preferred_date = None
    for cand in ["date", "post_date"]:
        if cand in df_cols_lower:
            preferred_date = df_cols_lower[cand]
            break
    if not preferred_date and date_keys:
        preferred_date = df_cols_lower[date_keys[0]]

    preferred_time = None
    for cand in ["time", "time (utc)"]:
        if cand in df_cols_lower:
            preferred_time = df_cols_lower[cand]
            break
    if not preferred_time and time_keys:
        preferred_time = df_cols_lower[time_keys[0]]

    if preferred_date:
        dval = row.get(preferred_date, None)
        tval = row.get(preferred_time, None) if preferred_time else None
        if pd.notna(dval) and str(dval).strip():
            if pd.notna(tval) and str(tval).strip():
                return f"{dval} {tval}"
            return dval

    # Some tools dump epoch under odd keys; as a last resort check any numeric-like col named *stamp*
    for col in row.index:
        if "stamp" in str(col).lower():
            val = row[col]
            if pd.notna(val):
                return val

    return None


def detect_platform_from_context(fname_lower: str, df_posts: pd.DataFrame | None) -> str:
    tokens = re.split(r"[^a-z0-9]+", fname_lower)
    tokens = [t for t in tokens if t]

    # filename hints
    if any(t in {"facebook", "fb"} for t in tokens):
        return "FB"
    if any(t in {"instagram", "ig"} for t in tokens):
        return "IG"
    if any(t in {"twitter", "x"} for t in tokens):
        return "X"

    # scan link columns if provided
    if df_posts is not None:
        for col in ("servicelink", "link", "permalink", "url", "post_url"):
            if col in df_posts.columns:
                series = df_posts[col].dropna().astype(str)
                if not series.empty:
                    sample = " ".join(series.head(20).tolist()).lower()
                    if "facebook.com" in sample:
                        return "FB"
                    if "instagram.com" in sample:
                        return "IG"
                    if "twitter.com" in sample or "x.com" in sample or "t.co/" in sample:
                        return "X"
    return "Unknown"


def to_int(x):
    """Robust int coerce for metrics."""
    if x is None:
        return 0
    try:
        return int(x)
    except Exception:
        try:
            return int(float(str(x).replace(",", "")))
        except Exception:
            return 0


# -----------------------
# Collections
# -----------------------
posts_data = []  # list of rows
daily_followers = defaultdict(dict)  # {platform: {date_str: total_fans}}

# -----------------------
# 1) Crawl CSV files
# -----------------------
for root, _, files in os.walk(base_dir):
    for filename in files:
        file_path = os.path.join(root, filename)
        if not filename.lower().endswith(".csv"):
            continue

        fname_lower = filename.lower()

        # --------- POSTS CSV ---------
        # skip summaries (e.g., "posts-summary-...csv") to avoid missing per-post timestamps
        if ("posts" in fname_lower) and ("summary" not in fname_lower) and all(
            k not in fname_lower for k in ("hashtags", "insights", "statistics", "demographic")
        ):
            try:
                df_posts = pd.read_csv(file_path)
            except Exception as e:
                logging.warning(f"Could not read posts CSV '{filename}': {e}")
                continue

            # Normalize column names
            orig_cols = list(df_posts.columns)
            df_posts.columns = [col.strip().lower() for col in df_posts.columns]
            col_map = {c.lower(): c for c in orig_cols}  # lower->original for row.get

            # Detect platform
            platform = detect_platform_from_context(fname_lower, df_posts)

            # Detect event name (path, filename, or text)
            event_name = None
            path_lower = root.lower()
            for key, name in event_keywords.items():
                if key in path_lower or key in fname_lower:
                    event_name = name
                    break

            if event_name is None and "text" in df_posts.columns:
                for text in df_posts["text"].astype(str):
                    low = text.lower()
                    for key, name in event_keywords.items():
                        if key in low:
                            event_name = name
                            break
                    if event_name:
                        break
            if event_name is None:
                event_name = "Unknown Event"

            # Process rows
            for _, row in df_posts.iterrows():
                # --- Timestamp extraction (robust) ---
                ts_val = extract_timestamp_from_row(row, col_map)
                jst_date, jst_hour, jst_dow = parse_to_jst_any(ts_val)
                if jst_date is None:
                    logging.warning(f"Skipping row without parsable date in {filename}: {ts_val}")
                    continue

                # URL normalization
                post_url = ""
                for url_col in ("servicelink", "post_url", "permalink", "link", "url"):
                    if url_col in df_posts.columns:
                        post_url = str(row.get(col_map.get(url_col, url_col), "") or "").strip()
                        if post_url:
                            break

                # Try constructing FB URL from ID if missing
                if not post_url and platform == "FB" and "id" in df_posts.columns:
                    fid = str(row.get(col_map.get("id", "id"), ""))
                    if "_" in fid:
                        post_url = f"https://facebook.com/{fid}"

                # Post format inference
                post_type = str(row.get(col_map.get("type", "type"), "") or "").lower()
                post_format = ""
                if post_type:
                    if platform == "IG":
                        if "video" in post_type:
                            if "/reel/" in post_url:
                                post_format = "Reel"
                            elif "/tv/" in post_url:
                                post_format = "Video"
                            else:
                                post_format = "Reel"
                        elif "carousel" in post_type or "album" in post_type:
                            post_format = "Carousel"
                        elif "image" in post_type or "photo" in post_type:
                            post_format = "Static"
                        else:
                            post_format = post_type.capitalize()
                    elif platform == "FB":
                        if "video" in post_type:
                            post_format = "Video"
                        elif "image" in post_type or "photo" in post_type:
                            media_field = row.get(col_map.get("media", "media"), "")
                            if isinstance(media_field, str) and media_field and any(sep in media_field for sep in ("|", ";")):
                                post_format = "Carousel"
                            else:
                                post_format = "Static"
                        elif "link" in post_type:
                            post_format = "Link"
                        else:
                            post_format = post_type.capitalize()
                    elif platform == "X":
                        if "video" in post_type:
                            post_format = "Video"
                        elif "image" in post_type or "photo" in post_type:
                            media_field = row.get(col_map.get("media", "media"), "")
                            if isinstance(media_field, str) and media_field and any(sep in media_field for sep in (",", "|")):
                                post_format = "Carousel"
                            else:
                                post_format = "Static"
                        elif "text" in post_type or "tweet" in post_type:
                            post_format = "Text"
                        else:
                            post_format = post_type.capitalize()
                    else:
                        post_format = post_type.capitalize()
                else:
                    if platform == "IG":
                        post_format = "Reel" if "/reel/" in post_url else "Static"
                    elif platform == "FB":
                        post_format = "Static"
                    elif platform == "X":
                        media_field = row.get(col_map.get("media", "media"), "")
                        if isinstance(media_field, str) and media_field:
                            if any(ext in media_field for ext in [".mp4", ".mov"]):
                                post_format = "Video"
                            elif any(sep in media_field for sep in (",", "|")):
                                post_format = "Carousel"
                            else:
                                post_format = "Static"
                        else:
                            post_format = "Text"
                    else:
                        post_format = ""

                # Hashtags (Unicode-friendly)
                caption_text = str(row.get(col_map.get("text", "text"), "") or "")
                hashtags_list = re.findall(r"#([^\s#]+)", caption_text)
                hashtags_raw = " ".join("#" + h for h in hashtags_list) if hashtags_list else ""

                # Theme from hashtags
                content_theme = ""
                for h in hashtags_list:
                    tag_key = h.lower()
                    for key, theme in theme_dict.items():
                        if key in tag_key:
                            content_theme = theme
                            break
                    if content_theme:
                        break

                # Metrics
                impressions_val = to_int(row.get(col_map.get("impressions", "impressions")))
                reach_val = to_int(row.get(col_map.get("reach", "reach")))
                likes_val = to_int(row.get(col_map.get("likes", "likes")))
                comments_val = to_int(row.get(col_map.get("comments", "comments")) or row.get(col_map.get("replies", "replies")))
                shares_val = to_int(row.get(col_map.get("shares", "shares")) or row.get(col_map.get("retweets", "retweets")))
                saves_val = to_int(row.get(col_map.get("saves", "saves")))
                reactions_val = to_int(row.get(col_map.get("reactions", "reactions")))

                # Engagements per platform
                if platform == "FB":
                    if row.get(col_map.get("reactions", "reactions")) is not None:
                        likes_val = reactions_val
                    engagements_val = likes_val + comments_val + shares_val
                elif platform == "IG":
                    engagements_val = likes_val + comments_val + shares_val + saves_val
                elif platform == "X":
                    engagements_val = likes_val + comments_val + shares_val
                else:
                    engagements_val = likes_val + comments_val + shares_val + saves_val

                engagement_rate_val = ""
                if impressions_val > 0:
                    engagement_rate_val = round((engagements_val / impressions_val) * 100, 2)

                link_clicks_val = to_int(row.get(col_map.get("clicks", "clicks")) or row.get(col_map.get("link_clicks", "link_clicks")))

                posts_data.append([
                    event_name,
                    platform,
                    jst_date,                     # Post Date (JST)
                    jst_hour,                     # Time of Day (hour) in JST
                    jst_dow,                      # Day of Week
                    post_url,
                    post_format or "",
                    hashtags_raw,
                    content_theme,
                    impressions_val if impressions_val != 0 else "",
                    reach_val if reach_val != 0 else "",
                    engagements_val if engagements_val != 0 else 0,
                    engagement_rate_val if engagement_rate_val != "" else "",
                    link_clicks_val if link_clicks_val != 0 else "",
                    0,  # Follows Gained (estimated) to fill later
                    filename,  # Notes / Source file
                ])

        # --------- INSIGHTS CSV (followers) ---------
        if "insights" in fname_lower:
            try:
                df_insights = pd.read_csv(file_path)
            except Exception as e:
                logging.warning(f"Could not read insights CSV '{filename}': {e}")
                continue

            plat = detect_platform_from_context(fname_lower, None)

            # Keep original col names, build lower->original map
            orig_cols = list(df_insights.columns)
            cols_lower = {c.lower(): c for c in orig_cols}

            date_candidates = [v for k, v in cols_lower.items() if "date" in k]
            total_candidates = [v for k, v in cols_lower.items() if ("followers" in k) or ("total" in k and "fan" in k)]

            if not date_candidates or not total_candidates:
                continue

            date_col = date_candidates[0]
            fans_col = total_candidates[0]

            for _, r in df_insights.iterrows():
                date_raw = r.get(date_col, "")
                try:
                    d = pd.to_datetime(date_raw, errors="raise")
                except Exception:
                    try:
                        d = pd.to_datetime(date_raw, format="%m/%d/%Y", errors="raise")
                    except Exception:
                        continue
                date_key = d.strftime("%m/%d/%Y")

                total_fans = to_int(r.get(fans_col))
                if total_fans > 0:
                    daily_followers[plat][date_key] = total_fans

# -----------------------
# 2) Parse Metricool PDFs (optional)
# -----------------------
pdf_posts_data = []
for root, _, files in os.walk(base_dir):
    for filename in files:
        if not filename.lower().endswith(".pdf"):
            continue
        pdf_path = os.path.join(root, filename)
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            logging.warning(f"Could not open PDF '{filename}': {e}")
            continue

        try:
            for page in doc:
                tables = page.find_tables()
                for table in tables:
                    data = table.extract()
                    if not data or not data[0]:
                        continue
                    header = [str(x).strip() for x in data[0]]
                    if any(h.lower().startswith("impression") for h in header):
                        df_table = pd.DataFrame(data[1:], columns=header)
                        df_table["_source_pdf"] = filename
                        pdf_posts_data.append(df_table)
        except Exception:
            pass
        finally:
            doc.close()

# -----------------------
# 3) Build posts_df (+ merge PDF if present)
# -----------------------
columns = [
    "Event", "Platform", "Post Date (JST)", "Time of Day (hour)", "Day of Week",
    "Post URL", "Format", "Hashtags", "Content Theme", "Impressions", "Reach",
    "Engagements", "Engagement Rate", "Link Clicks", "Follows Gained (estimated)",
    "Notes / Source file",
]
if pdf_posts_data:
    pdf_posts_df = pd.concat(pdf_posts_data, ignore_index=True)
    pdf_posts_df.columns = [c.strip().lower() for c in pdf_posts_df.columns]
    if "post url" in pdf_posts_df.columns:
        pdf_posts_df = pdf_posts_df.rename(columns={"post url": "post_url"})

    unified_df = pd.DataFrame(posts_data, columns=columns)

    if "post_url" in pdf_posts_df.columns:
        keep_cols = [c for c in ("post_url", "impressions", "reach", "engagements") if c in pdf_posts_df.columns]
        if keep_cols and "post_url" in keep_cols:
            unified_df = unified_df.merge(
                pdf_posts_df[keep_cols],
                how="left",
                left_on="Post URL",
                right_on="post_url",
                copy=False,
                validate="m:1",
            )
            if "impressions" in unified_df.columns:
                unified_df["Impressions"] = unified_df["Impressions"].fillna(unified_df["impressions"])
            if "reach" in unified_df.columns:
                unified_df["Reach"] = unified_df["Reach"].fillna(unified_df["reach"])
            if "engagements" in unified_df.columns:
                unified_df["Engagements"] = unified_df["Engagements"].fillna(unified_df["engagements"])
            unified_df = unified_df.drop(columns=[c for c in ("post_url", "impressions", "reach", "engagements") if c in unified_df.columns])

    posts_df = unified_df.copy()
else:
    posts_df = pd.DataFrame(posts_data, columns=columns)

# -----------------------
# 4) De-duplicate posts
# -----------------------
posts_df["__dedupe_key"] = posts_df["Post URL"].fillna("").str.strip()
mask_no_url = posts_df["__dedupe_key"] == ""
fallback_key = (
    posts_df["Platform"].astype(str)
    + "|" + posts_df["Post Date (JST)"].astype(str)
    + "|" + posts_df["Time of Day (hour)"].astype(str)
    + "|" + posts_df["Hashtags"].astype(str).str[:60]
)
posts_df.loc[mask_no_url, "__dedupe_key"] = fallback_key[mask_no_url]
posts_df = posts_df.drop_duplicates(subset="__dedupe_key").drop(columns="__dedupe_key")

# -----------------------
# 5) Followers: compute daily gains per platform & attribute by platform+day
# -----------------------
daily_new_followers = {}  # {platform: {date: diff}}
for plat, series in daily_followers.items():
    gains = {}
    dates_sorted = sorted(series.keys(), key=lambda x: datetime.strptime(x, "%m/%d/%Y"))
    prev = None
    for d in dates_sorted:
        curr = series[d]
        if prev is None:
            prev = curr
            continue
        diff = curr - prev
        if diff < 0 and abs(diff) > 0.9 * max(prev, 1):  # guard against data glitches
            diff = 0
        gains[d] = diff
        prev = curr
    daily_new_followers[plat] = gains

if daily_new_followers:
    posts_df["Engagements"] = pd.to_numeric(posts_df["Engagements"], errors="coerce").fillna(0)
    for (plat, pdate), group in posts_df.groupby(["Platform", "Post Date (JST)"], observed=False, dropna=False):
        new_followers = daily_new_followers.get(plat, {}).get(pdate, 0)
        if new_followers > 0:
            total_eng = group["Engagements"].sum()
            if total_eng > 0:
                share = group["Engagements"] / total_eng
                allocated = (share * new_followers).round().astype(int)
                posts_df.loc[group.index, "Follows Gained (estimated)"] = allocated
            else:
                posts_df.loc[group.index, "Follows Gained (estimated)"] = 0
        else:
            posts_df.loc[group.index, "Follows Gained (estimated)"] = 0

# -----------------------
# 6) Save unified + per-platform CSVs
# -----------------------
posts_df.to_csv("unified_post_data.csv", index=False)
print("Unified post data saved to unified_post_data.csv")

for plat in ["IG", "FB", "X", "Unknown"]:
    sub = posts_df[posts_df["Platform"] == plat]
    if not sub.empty:
        out = f"unified_post_data_{plat}.csv"
        sub.to_csv(out, index=False)
        print(f"Saved per-platform posts: {out}")

# -----------------------
# 7) Numeric coercion, DOW order, JST Hour alias
# -----------------------
metric_cols = ["Impressions", "Reach", "Engagements", "Engagement Rate", "Link Clicks", "Follows Gained (estimated)"]
for c in metric_cols:
    posts_df[c] = pd.to_numeric(posts_df[c], errors="coerce")

posts_df["Day of Week"] = pd.Categorical(posts_df["Day of Week"], categories=DOW_ORDER, ordered=True)
posts_df["JST Hour"] = posts_df["Time of Day (hour)"]

# -----------------------
# 8) Summaries (per-platform + combined)
# -----------------------
summary_format = posts_df.copy()

format_summary_df = (summary_format.groupby("Format", observed=False)
                     .agg(**{
                         "Posts Count": ("Post URL", "count"),
                         "Avg Impressions": ("Impressions", "mean"),
                         "Avg Reach": ("Reach", "mean"),
                         "Avg Engagements": ("Engagements", "mean"),
                         "Avg Engagement Rate": ("Engagement Rate", "mean"),
                         "Avg Link Clicks": ("Link Clicks", "mean"),
                         "Avg Follows Gained": ("Follows Gained (estimated)", "mean"),
                     })).reset_index()
format_summary_df.to_csv("summary_by_format.csv", index=False)
print("Summary by format saved to summary_by_format.csv")

dow_summary_df = (summary_format.groupby("Day of Week", observed=False)
                  .agg(**{
                      "Posts Count": ("Post URL", "count"),
                      "Avg Impressions": ("Impressions", "mean"),
                      "Avg Reach": ("Reach", "mean"),
                      "Avg Engagements": ("Engagements", "mean"),
                      "Avg Engagement Rate": ("Engagement Rate", "mean"),
                      "Avg Link Clicks": ("Link Clicks", "mean"),
                      "Avg Follows Gained": ("Follows Gained (estimated)", "mean"),
                  })).reset_index()
dow_summary_df.to_csv("summary_by_dayofweek.csv", index=False)
print("Summary by day of week saved to summary_by_dayofweek.csv")

theme_summary_df = (summary_format.groupby("Content Theme", observed=False)
                    .agg(**{
                        "Posts Count": ("Post URL", "count"),
                        "Avg Impressions": ("Impressions", "mean"),
                        "Avg Reach": ("Reach", "mean"),
                        "Avg Engagements": ("Engagements", "mean"),
                        "Avg Engagement Rate": ("Engagement Rate", "mean"),
                        "Avg Link Clicks": ("Link Clicks", "mean"),
                        "Avg Follows Gained": ("Follows Gained (estimated)", "mean"),
                    })).reset_index()
theme_summary_df.to_csv("summary_by_theme.csv", index=False)
print("Summary by content theme saved to summary_by_theme.csv")

# --------- Per-platform summaries ---------
for plat, g in posts_df.groupby("Platform", observed=False):
    if g.empty:
        continue
    pf = (g.groupby("Format", observed=False)
          .agg(**{
              "Posts Count": ("Post URL", "count"),
              "Avg Impressions": ("Impressions", "mean"),
              "Avg Reach": ("Reach", "mean"),
              "Avg Engagements": ("Engagements", "mean"),
              "Avg Engagement Rate": ("Engagement Rate", "mean"),
              "Avg Link Clicks": ("Link Clicks", "mean"),
              "Avg Follows Gained": ("Follows Gained (estimated)", "mean"),
          })).reset_index()
    pf.to_csv(f"summary_by_format_{plat}.csv", index=False)

    pdow = (g.groupby("Day of Week", observed=False)
            .agg(**{
                "Posts Count": ("Post URL", "count"),
                "Avg Impressions": ("Impressions", "mean"),
                "Avg Reach": ("Reach", "mean"),
                "Avg Engagements": ("Engagements", "mean"),
                "Avg Engagement Rate": ("Engagement Rate", "mean"),
                "Avg Link Clicks": ("Link Clicks", "mean"),
                "Avg Follows Gained": ("Follows Gained (estimated)", "mean"),
            })).reset_index()
    pdow.to_csv(f"summary_by_dayofweek_{plat}.csv", index=False)

    ptheme = (g.groupby("Content Theme", observed=False)
              .agg(**{
                  "Posts Count": ("Post URL", "count"),
                  "Avg Impressions": ("Impressions", "mean"),
                  "Avg Reach": ("Reach", "mean"),
                  "Avg Engagements": ("Engagements", "mean"),
                  "Avg Engagement Rate": ("Engagement Rate", "mean"),
                  "Avg Link Clicks": ("Link Clicks", "mean"),
                  "Avg Follows Gained": ("Follows Gained (estimated)", "mean"),
              })).reset_index()
    ptheme.to_csv(f"summary_by_theme_{plat}.csv", index=False)

print("Per-platform summaries saved (by format/day/theme).")

# -----------------------
# 9) Best time (JST) per platform
# -----------------------
hourly = (posts_df
          .groupby(["Platform", "JST Hour"], dropna=False, observed=False)
          .agg(posts=("Post URL", "count"),
               med_eng_rate=("Engagement Rate", "median"),
               mean_impr=("Impressions", "mean"))
          .reset_index()
          .sort_values(["Platform", "med_eng_rate", "mean_impr", "posts"], ascending=[True, False, False, False]))
hourly.to_csv("best_hours_by_platform.csv", index=False)
print("Best hours by platform saved to best_hours_by_platform.csv")

heatmaps = []
for plat, g in posts_df.groupby("Platform", observed=False):
    pivot = g.pivot_table(index="Day of Week",
                          columns="JST Hour",
                          values="Engagement Rate",
                          aggfunc="median",
                          observed=False)
    pivot["Platform"] = plat
    heatmaps.append(pivot.reset_index())
if heatmaps:
    heatmap_df = pd.concat(heatmaps, ignore_index=True)
    heatmap_df.to_csv("best_times_heatmap_by_platform.csv", index=False)
    print("Best times heatmap saved to best_times_heatmap_by_platform.csv")

triple = (posts_df
          .groupby(["Platform", "Day of Week", "JST Hour"], dropna=False, observed=False)
          .agg(posts=("Post URL", "count"),
               med_eng_rate=("Engagement Rate", "median"),
               mean_impr=("Impressions", "mean"))
          .reset_index()
          .sort_values(["Platform", "med_eng_rate", "mean_impr", "posts"], ascending=[True, False, False, False]))
triple.to_csv("best_times_by_platform_day_hour.csv", index=False)
print("Best times by platform/day/hour saved to best_times_by_platform_day_hour.csv")
