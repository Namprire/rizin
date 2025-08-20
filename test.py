import os
import re
import fitz  # PyMuPDF for PDF parsing
import pandas as pd
from datetime import datetime

# Define the base directory to crawl (current directory by default)
base_dir = "."  # adjust if needed

# Keyword-to-event mapping for identifying event names from file paths or text
event_keywords = {
    "korea": "Rizin South Korea",
    "ws_korea": "Rizin South Korea",
    "landmark": "Rizin Landmark",
    "rizin 4": "Rizin 4",
    "rizin4": "Rizin 4"
}

# Keyword-to-theme mapping for content themes (editable as needed)
theme_dict = {
    "fightorder": "FightOrder",
    "behindthescenes": "BehindTheScenes",
    # Add other keywords for themes as needed, e.g.:
    # "weighin": "WeighIn",
    # "faceoff": "FaceOff",
    # "highlights": "Highlights",
    # etc.
}

# Containers for post data and follower counts
posts_data = []            # will collect each post's info as a list
daily_followers = {}       # to store total followers per date (for follower gain calc)

# 1. Crawl for relevant CSV files (Buffer exports or Metricool CSVs)
for root, _, files in os.walk(base_dir):
    for filename in files:
        file_path = os.path.join(root, filename)
        # Identify post-level CSV files (exclude non-post data like demographics or aggregated stats)
        if filename.lower().endswith(".csv"):
            fname_lower = filename.lower()
            if ("posts" in fname_lower) and ("hashtags" not in fname_lower 
                                             and "insights" not in fname_lower 
                                             and "statistics" not in fname_lower 
                                             and "demographic" not in fname_lower):
                # Read the posts CSV
                try:
                    df_posts = pd.read_csv(file_path)
                except Exception as e:
                    print(f"Warning: Could not read {filename}: {e}")
                    continue
                # Normalize column names (strip and lower-case for consistency)
                df_posts.columns = [col.strip().lower() for col in df_posts.columns]
                
                # Determine platform from file context or content
                platform = "Unknown"
                if "facebook" in fname_lower or "fb" in fname_lower:
                    platform = "FB"
                elif "instagram" in fname_lower or "ig" in fname_lower:
                    platform = "IG"
                elif "twitter" in fname_lower or fname_lower.startswith("x") or "tw" in fname_lower:
                    platform = "X"
                # If not inferred from name, try using any link or ID pattern in data
                if platform == "Unknown":
                    if 'servicelink' in df_posts.columns:
                        # Check the domain in the first non-null service link
                        first_link = df_posts['servicelink'].dropna().astype(str)
                        if len(first_link) > 0:
                            first_link = first_link.iloc[0]
                        else:
                            first_link = ""
                        if "facebook.com" in first_link:
                            platform = "FB"
                        elif "instagram.com" in first_link:
                            platform = "IG"
                        elif "twitter.com" in first_link or "t.co/" in first_link or "x.com" in first_link:
                            platform = "X"
                
                # Determine event name from folder path or file name or post content
                event_name = None
                path_lower = root.lower()
                # Check folder path and filename for event keywords
                for key, name in event_keywords.items():
                    if key in path_lower or key in fname_lower:
                        event_name = name
                        break
                # If still not found, try to infer from post text hashtags
                if event_name is None and 'text' in df_posts.columns:
                    for text in df_posts['text'].astype(str):
                        low_text = text.lower()
                        for key, name in event_keywords.items():
                            if key in low_text:
                                event_name = name
                                break
                        if event_name:
                            break
                if event_name is None:
                    event_name = "Unknown Event"
                
                # Process each post entry
                for _, row in df_posts.iterrows():
                    # Parse post datetime string to datetime object
                    # Assuming dates are given in a standard format like MM/DD/YYYY HH:MM:SS
                    date_str = str(row.get('date', ''))  # get 'date' column
                    if not date_str:
                        continue  # skip if no date
                    try:
                        # Use datetime.strptime with common patterns or fall back to dateutil parser
                        # Try multiple possible formats as needed
                        try:
                            # Common format example: '05/31/2025 15:07:30'
                            dt = datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
                        except ValueError:
                            # Try without time or other format variations
                            dt = datetime.strptime(date_str, "%m/%d/%Y")
                        post_date = dt.strftime("%m/%d/%Y")             # format date as MM/DD/YYYY (JST)
                        post_hour = dt.hour                              # hour of day (0-23)
                        day_of_week = dt.strftime("%A")                  # day name, e.g. 'Friday'
                    except Exception as e:
                        # If parsing fails, skip this post
                        print(f"Could not parse date '{date_str}' in {filename}: {e}")
                        continue
                    
                    # Compose Post URL if available (Buffer exports often have 'servicelink' or similar)
                    post_url = ""
                    if 'servicelink' in df_posts.columns:
                        post_url = str(row.get('servicelink', ''))
                    elif 'link' in df_posts.columns:
                        post_url = str(row.get('link', ''))
                    else:
                        # If no direct link, perhaps construct from ID if platform is FB (ID looks like pageid_postid)
                        if platform == "FB" and 'id' in df_posts.columns:
                            fid = str(row.get('id', ''))
                            if '_' in fid:
                                # Construct a Facebook URL if possible
                                post_url = f"https://facebook.com/{fid}"
                        # (For other platforms, if needed, could add logic to build URL from ID or skip)
                    
                    # Determine post format (Reel, Carousel, Static, Video, etc.)
                    post_type = str(row.get('type', '')).lower()
                    post_format = ""
                    # Use 'type' field if present (Buffer Analyze exports often provide a post type)
                    if post_type:
                        if platform == "IG":
                            # Instagram types could be 'image', 'video', etc.
                            if "video" in post_type:
                                # Check if URL indicates a Reel
                                if "/reel/" in post_url:
                                    post_format = "Reel"
                                elif "/tv/" in post_url:
                                    post_format = "Video"  # IGTV or older video format
                                else:
                                    # If not explicitly a Reel link, treat any video as Reel (Instagram now treats most videos as Reels)
                                    post_format = "Reel"
                            elif "carousel" in post_type or "album" in post_type:
                                post_format = "Carousel"
                            elif "image" in post_type or "photo" in post_type:
                                post_format = "Static"
                            else:
                                post_format = post_type.capitalize()
                        elif platform == "FB":
                            # Facebook types might be 'video', 'photo', 'link', etc.
                            if "video" in post_type:
                                post_format = "Video"
                            elif "image" in post_type or "photo" in post_type:
                                # Check if multiple images (Buffer might not explicitly label albums; we can infer if media list has multiple items)
                                media_field = row.get('media', '')
                                if isinstance(media_field, str) and media_field and ("|" in media_field or ";" in media_field):
                                    post_format = "Carousel"
                                else:
                                    post_format = "Static"
                            elif "link" in post_type:
                                post_format = "Link"
                            else:
                                post_format = post_type.capitalize()
                        elif platform == "X":
                            # Twitter (X) posts might be 'text', 'image', 'video' as type if provided
                            if "video" in post_type:
                                post_format = "Video"
                            elif "image" in post_type or "photo" in post_type:
                                # Check if multiple images (Twitter allows up to 4 photos)
                                media_field = row.get('media', '')
                                if isinstance(media_field, str) and media_field and ("," in media_field or "|" in media_field):
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
                        # If no explicit 'type', try to infer from media or platform defaults
                        if platform == "IG":
                            post_format = "Reel" if "/reel/" in post_url else "Static"
                        elif platform == "FB":
                            post_format = "Static"
                        elif platform == "X":
                            # If media links present in Twitter data, infer
                            media_field = row.get('media', '')
                            if isinstance(media_field, str) and media_field:
                                # Check for video file indications
                                if any(ext in media_field for ext in [".mp4", ".mov"]):
                                    post_format = "Video"
                                elif "," in media_field or "|" in media_field:
                                    post_format = "Carousel"
                                else:
                                    post_format = "Static"
                            else:
                                post_format = "Text"
                        else:
                            post_format = ""
                    
                    # Extract hashtags from the post text/caption
                    caption_text = str(row.get('text', ''))
                    hashtags_list = re.findall(r"#\w[\w\d_]*", caption_text)
                    hashtags_raw = " ".join(hashtags_list) if hashtags_list else ""
                    
                    # Determine content theme by checking hashtags against theme_dict
                    content_theme = ""
                    for tag in hashtags_list:
                        tag_key = tag.lower().strip('#')
                        for key, theme in theme_dict.items():
                            if key in tag_key:
                                content_theme = theme
                                break
                        if content_theme:
                            break  # assign the first matching theme and break
                    # (If multiple themes apply, we could concatenate or choose priority. Currently picking first match.)
                    
                    # Retrieve metrics: impressions, reach, likes, comments, shares, saves, etc.
                    # Use 0 if not available to avoid None in calculations
                    impressions = row.get('impressions', None)
                    reach = row.get('reach', None)
                    likes = row.get('likes', None)
                    comments = row.get('comments', None)
                    shares = None
                    saves = row.get('saves', None)
                    reactions = row.get('reactions', None)
                    # Twitter data might use 'retweets' or 'replies' columns instead
                    if shares is None:
                        shares = row.get('shares', None) or row.get('retweets', None)
                    # If Twitter replies are not captured under 'comments', check 'replies'
                    replies = row.get('replies', None)
                    if comments is None and replies is not None:
                        comments = replies
                    
                    # Convert metrics to numeric values (if string or float)
                    def to_int(x):
                        # Convert to int if possible (handle floats or strings that represent numbers)
                        if x is None or x != x:  # check for None or NaN (NaN != NaN)
                            return 0
                        try:
                            return int(x)
                        except:
                            try:
                                return int(float(x))
                            except:
                                return 0
                    
                    impressions_val = to_int(impressions)
                    reach_val = to_int(reach)
                    likes_val = to_int(likes)
                    comments_val = to_int(comments)
                    shares_val = to_int(shares)
                    saves_val = to_int(saves)
                    reactions_val = to_int(reactions)
                    
                    # Calculate engagements = likes + comments + shares + saves (platform-specific adjustments):
                    engagements_val = 0
                    if platform == "FB":
                        # For Facebook, use total reactions (if available) instead of just likes to capture all reaction types
                        if reactions is not None:
                            likes_val = reactions_val  # replace likes count with total reactions count
                        engagements_val = likes_val + comments_val + shares_val
                        # (Facebook typically doesn't have "saves" for feed posts, so we ignore saves_val for FB)
                    elif platform == "IG":
                        # Instagram engagements include likes, comments, shares (usually story reshares if provided) and saves
                        engagements_val = likes_val + comments_val + shares_val + saves_val
                    elif platform == "X":
                        # Twitter engagements: likes + replies + retweets (shares). We include comments_val which may already have replies.
                        engagements_val = likes_val + comments_val + shares_val
                        # (Twitter link clicks or profile clicks are excluded from this engagement definition)
                    else:
                        # Default: sum what we have
                        engagements_val = likes_val + comments_val + shares_val + saves_val
                    
                    # Compute engagement rate (engagements as percentage of impressions)
                    engagement_rate_val = ""
                    if impressions_val and impressions_val > 0:
                        engagement_rate_val = round((engagements_val / impressions_val) * 100, 2)
                    
                    # Link clicks (if available)
                    link_clicks_val = to_int(row.get('clicks', None) or row.get('link_clicks', None))
                    
                    # Prepare the source note (which file or source this post came from)
                    source_note = filename  # the CSV file name as source indicator
                    
                    # Append the normalized post record to the list
                    posts_data.append([
                        event_name,
                        platform,
                        post_date,
                        post_hour,
                        day_of_week,
                        post_url,
                        post_format or "",
                        hashtags_raw,
                        content_theme,
                        impressions_val if impressions_val != 0 else "",
                        reach_val if reach_val != 0 else "",
                        engagements_val if engagements_val != 0 else 0,  # use 0 for no engagement
                        engagement_rate_val if engagement_rate_val != "" else "",
                        link_clicks_val if link_clicks_val != 0 else "",
                        0,  # placeholder for Follows Gained (estimated), to be calculated later
                        source_note
                    ])

        # Also collect Metricool "insights" data for daily followers (if CSV)
        if filename.lower().endswith(".csv") and "insights" in filename.lower():
            try:
                df_insights = pd.read_csv(file_path)
            except Exception as e:
                continue
            # If the insights CSV contains total followers by date, extract that
            # We look for columns that might contain total follower count (e.g., 'Total Fans' or 'Followers')
            col_candidates = [c for c in df_insights.columns if "total" in c.lower() and "fan" in c.lower() or "followers" in c.lower()]
            date_col = [c for c in df_insights.columns if "date" in c.lower()]
            if col_candidates and date_col:
                date_col = date_col[0]
                fans_col = col_candidates[0]
                # Ensure sorting by date if not already
                for _, row in df_insights.iterrows():
                    date_str = str(row[date_col])
                    # Normalize date format to MM/DD/YYYY (assuming insights given in similar format)
                    try:
                        dt = datetime.strptime(date_str, "%m/%d/%Y")
                        date_key = dt.strftime("%m/%d/%Y")
                    except:
                        date_key = date_str
                    total_fans = None
                    try:
                        total_fans = int(row[fans_col])
                    except:
                        # Handle potential numeric issues (commas or NaNs)
                        try:
                            total_fans = int(str(row[fans_col]).replace(",", ""))
                        except:
                            continue
                    daily_followers[date_key] = total_fans

# 2. Parse Metricool PDF reports for additional insights (if any PDF files exist)
# Metricool PDFs may contain tables of post metrics or hashtag performance.
pdf_posts_data = []  # to hold data extracted from PDFs, if needed for merging
for root, _, files in os.walk(base_dir):
    for filename in files:
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(root, filename)
            try:
                doc = fitz.open(pdf_path)
            except Exception as e:
                print(f"Could not open PDF {filename}: {e}")
                continue
            text_content = ""
            # Attempt to extract structured tables if available
            try:
                for page in doc:
                    tables = page.find_tables()
                    for table in tables:
                        data = table.extract()  # extract table as list of lists
                        # If the first row contains column names like 'Impressions', we assume this is a post metrics table
                        header = [str(x).strip() for x in data[0]]
                        if any(h.lower().startswith("impression") for h in header):
                            df_table = pd.DataFrame(data[1:], columns=header)
                            # Add an identifier for which PDF this came from
                            df_table['_source_pdf'] = filename
                            pdf_posts_data.append(df_table)
            except Exception as e:
                # If table extraction fails, fallback to text extraction
                for page in doc:
                    text_content += page.get_text()
            # If we have text_content (no table extracted or additional info), 
            # we could parse text for needed metrics (this may require custom parsing depending on PDF format).
            # For brevity, not fully implementing text parsing here.
            # (If needed, parse `text_content` to find lines for each post and extract numbers.)
            doc.close()

# If PDF tables were extracted, merge them into one DataFrame
if pdf_posts_data:
    pdf_posts_df = pd.concat(pdf_posts_data, ignore_index=True)
    # Clean up columns names to lower-case
    pdf_posts_df.columns = [col.strip().lower() for col in pdf_posts_df.columns]
    # Potentially use this to fill missing metrics in posts_data by matching on post URL or date/time & text.
    # For example, if Buffer CSV lacked impressions, we could merge on date and maybe partial text to add impressions.
    # (Implementation of matching logic depends on data consistency; ensure keys exist to join on.)
    # Here we demonstrate a simple merge on post URL if available in PDF data:
    if 'post url' in pdf_posts_df.columns:
        # normalize URL column name
        pdf_posts_df.rename(columns={'post url': 'post_url'}, inplace=True)
    # If PDF provides a text snippet or caption, we could try to match by that if URL or ID not present.
    # ... (matching logic to be implemented as needed) ...
    
    # As an example, we merge on post_url if present:
    unified_df = pd.DataFrame(posts_data, columns=[
        "Event", "Platform", "Post Date (JST)", "Time of Day (hour)", "Day of Week",
        "Post URL", "Format", "Hashtags", "Content Theme", "Impressions", "Reach",
        "Engagements", "Engagement Rate", "Link Clicks", "Follows Gained (estimated)",
        "Notes / Source file"
    ])
    if 'post_url' in pdf_posts_df.columns and 'impressions' in pdf_posts_df.columns:
        # Left-join to add any missing metrics from PDF data
        unified_df = unified_df.merge(pdf_posts_df[['post_url', 'impressions', 'reach', 'engagements']], 
                                      how='left', left_on='Post URL', right_on='post_url')
        # If any impressions or reach were NaN in unified_df and present in pdf data, fill them
        unified_df['Impressions'] = unified_df['Impressions'].fillna(unified_df['impressions'])
        unified_df['Reach'] = unified_df['Reach'].fillna(unified_df['reach'])
        unified_df['Engagements'] = unified_df['Engagements'].fillna(unified_df['engagements'])
        # Drop the extra merge columns
        unified_df.drop(columns=['post_url', 'impressions', 'reach', 'engagements'], inplace=True)
    posts_df = unified_df.copy()
else:
    # No PDF data to merge, just use posts_data from CSVs
    posts_df = pd.DataFrame(posts_data, columns=[
        "Event", "Platform", "Post Date (JST)", "Time of Day (hour)", "Day of Week",
        "Post URL", "Format", "Hashtags", "Content Theme", "Impressions", "Reach",
        "Engagements", "Engagement Rate", "Link Clicks", "Follows Gained (estimated)",
        "Notes / Source file"
    ])

# 3. Follower attribution: distribute daily new followers to posts
# First, compute daily follower gains from the collected daily_followers data
daily_new_followers = {}  # net new followers per day
if daily_followers:
    # Sort the dates to ensure correct order for diff
    dates_sorted = sorted(daily_followers.keys(), key=lambda x: datetime.strptime(x, "%m/%d/%Y"))
    prev_value = None
    for date in dates_sorted:
        curr_value = daily_followers[date]
        if prev_value is None:
            # no previous day (first recorded day), skip diff calculation
            prev_value = curr_value
            continue
        diff = curr_value - prev_value
        # If there's an obvious data error (e.g., huge drop followed by same increase), handle that:
        if diff < 0 and abs(diff) > 0.9 * prev_value and date in daily_followers:
            # If a drop is nearly the entire follower count (likely erroneous), treat diff as 0
            diff = 0
        daily_new_followers[date] = diff
        prev_value = curr_value

# Now assign follower gains to posts for each day
if daily_new_followers:
    # Group posts by date
    for post_date, group in posts_df.groupby("Post Date (JST)"):
        # Use the net new followers on that date (if negative or missing, treat as 0 for attribution)
        new_followers = daily_new_followers.get(post_date, 0)
        if new_followers > 0:
            total_engagements = group["Engagements"].astype(float).sum()
            if total_engagements > 0:
                # Distribute followers proportional to engagement share
                # Calculate each post's share of engagements
                share = group["Engagements"].astype(float) / total_engagements
                # Allocate followers (as float initially)
                allocated = share * new_followers
                # Assign the rounded allocation back to the DataFrame
                posts_df.loc[group.index, "Follows Gained (estimated)"] = allocated.round().astype(int)
            else:
                # If no engagement on that day, no attribution (all remain 0)
                posts_df.loc[group.index, "Follows Gained (estimated)"] = 0
        else:
            # No positive follower gain to attribute (or net loss/zero) => leave as 0
            posts_df.loc[group.index, "Follows Gained (estimated)"] = 0

# 4. Save the unified post-level data to CSV
output_csv = "unified_post_data.csv"
posts_df.to_csv(output_csv, index=False)
print(f"Unified post data saved to {output_csv}")

# 5. (Optional) Create summary performance CSVs by Format, Day of Week, and Content Theme
summary_format = posts_df.copy()
# Convert numeric columns to numeric types for aggregation (if not already)
numeric_cols = ["Impressions", "Reach", "Engagements", "Engagement Rate", "Link Clicks", "Follows Gained (estimated)"]
for col in numeric_cols:
    # coerce errors to NaN for safety
    summary_format[col] = pd.to_numeric(summary_format[col], errors='coerce')
# Group by Format
format_group = summary_format.groupby("Format")
format_summary_df = format_group.agg({
    "Post URL": "count",
    "Impressions": "mean",
    "Reach": "mean",
    "Engagements": "mean",
    "Engagement Rate": "mean",
    "Link Clicks": "mean",
    "Follows Gained (estimated)": "mean"
}).reset_index()
format_summary_df.rename(columns={"Post URL": "Posts Count",
                                  "Impressions": "Avg Impressions",
                                  "Reach": "Avg Reach",
                                  "Engagements": "Avg Engagements",
                                  "Engagement Rate": "Avg Engagement Rate",
                                  "Link Clicks": "Avg Link Clicks",
                                  "Follows Gained (estimated)": "Avg Follows Gained"}, inplace=True)
format_summary_df.to_csv("summary_by_format.csv", index=False)
print("Summary by format saved to summary_by_format.csv")

# Group by Day of Week
dow_group = summary_format.groupby("Day of Week")
dow_summary_df = dow_group.agg({
    "Post URL": "count",
    "Impressions": "mean",
    "Reach": "mean",
    "Engagements": "mean",
    "Engagement Rate": "mean",
    "Link Clicks": "mean",
    "Follows Gained (estimated)": "mean"
}).reset_index()
dow_summary_df.rename(columns={"Post URL": "Posts Count",
                               "Impressions": "Avg Impressions",
                               "Reach": "Avg Reach",
                               "Engagements": "Avg Engagements",
                               "Engagement Rate": "Avg Engagement Rate",
                               "Link Clicks": "Avg Link Clicks",
                               "Follows Gained (estimated)": "Avg Follows Gained"}, inplace=True)
dow_summary_df.to_csv("summary_by_dayofweek.csv", index=False)
print("Summary by day of week saved to summary_by_dayofweek.csv")

# Group by Content Theme
theme_group = summary_format.groupby("Content Theme")
theme_summary_df = theme_group.agg({
    "Post URL": "count",
    "Impressions": "mean",
    "Reach": "mean",
    "Engagements": "mean",
    "Engagement Rate": "mean",
    "Link Clicks": "mean",
    "Follows Gained (estimated)": "mean"
}).reset_index()
theme_summary_df.rename(columns={"Post URL": "Posts Count",
                                 "Impressions": "Avg Impressions",
                                 "Reach": "Avg Reach",
                                 "Engagements": "Avg Engagements",
                                 "Engagement Rate": "Avg Engagement Rate",
                                 "Link Clicks": "Avg Link Clicks",
                                 "Follows Gained (estimated)": "Avg Follows Gained"}, inplace=True)
theme_summary_df.to_csv("summary_by_theme.csv", index=False)
print("Summary by content theme saved to summary_by_theme.csv")
