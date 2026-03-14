import requests
import json
import re
import pandas as pd
import time
import random
import urllib.parse
import matplotlib.pyplot as plt
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as XLImage
import os




import os

def append_json(filename, data):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = {}
    else:
        existing = {}

    existing.update(data)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)




# ✅ Auto Keyword Combination (Topic only, Location only, Topic+Location)
topic_keywords = [
    "Belt and Road Initiative (BRI)", "One Belt One Road (OBOR)", "Maritime Silk Road",
    "Silk Road Economic Belt", "Chinese investment", "Chinese infrastructure projects",
    "Debt diplomacy", "Economic cooperation", "Strategic partnerships",
    "一带一路倡议 (BRI)", "一带一路 (OBOR)", "海上丝绸之路", "丝绸之路经济带",
    "中国投资", "中国基础设施项目", "债务外交", "经济合作", "战略伙伴关系",
    "一帶一路倡議", "一帶一路", "海上絲路", "絲路經濟帶",
    "中國投資", "中國基礎建設項目", "債務外交", "經濟合作", "戰略夥伴關係"
]

location_keywords = [
    "Papua New Guinea (PNG)", "Solomon Islands (SI)", "Vanuatu", "Fiji",
    "巴布亚新几内亚 (PNG)", "所罗门群岛 (SI)", "瓦努阿图", "斐济",
    "巴布亞紐幾內亞 (PNG)", "所羅門群島 (SI)", "瓦努阿圖", "斐濟"
]

keywords_topic_only = topic_keywords
keywords_location_only = location_keywords
keywords_topic_location = [f"{t} {l}" for t in topic_keywords for l in location_keywords]
keywords = keywords_topic_only + keywords_location_only + keywords_topic_location
print(f"🔢 Total keywords: {len(keywords)}")


# ✅ Load checkpoint
try:
    with open("completed_keywords.json", "r", encoding="utf-8") as f:
        completed_keywords = set(json.load(f))
except FileNotFoundError:
    completed_keywords = set()
    


# 🍪 Cookies (Update if expired)
cookies = {
    "SESSDATA": "",
    "bili_jct": "",
    "DedeUserID": ""
}

# Request headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://www.bilibili.com/",
    "Origin": "https://www.bilibili.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive"
}

# Use a session to send requests
session = requests.Session()
session.cookies.update(cookies)
session.keep_alive = True


# 📁 Initialize combined video data list
failed_comments_log = []  # Log for videos whose comments failed to fetch

def human_sleep(min_sec=0.5, max_sec=2.5):
    base_sleep = random.uniform(min_sec, max_sec)
    if random.random() < 0.1:
        base_sleep += random.uniform(2, 5)
    time.sleep(base_sleep)


# 🔁 Load checkpoint if exists
completed_keywords = set()
completed_comments = set()
all_video_records = []
all_comments_combined = []
all_raw_json = {}
all_user_json = {}
start_time = time.time()
try:
    with open("completed_keywords.json", "r", encoding="utf-8") as f:
        completed_keywords = set(json.load(f))
except FileNotFoundError:
    pass

for keyword in keywords:
    if keyword in completed_keywords:
        print(f"⏩ Skipping already completed keyword: {keyword}")
        continue
    print(f"🔍 Collecting videos for keyword: {keyword}")
    encoded_keyword = urllib.parse.quote(keyword)
    search_url_template = "https://api.bilibili.com/x/web-interface/search/type?keyword={}&search_type=video&page={}&ps=20"

    all_videos = []
    max_pages = 20
    target_count = 200
    collected = 0
    raw_json_per_keyword = []

    for page in range(1, max_pages + 1):
        search_url = search_url_template.format(encoded_keyword, page)
        for attempt in range(3):
            try:
                response = session.get(search_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    break
                else:
                    print(f"⛔️ Status {response.status_code}, retrying page {page}...")
            except Exception as e:
                print(f"⛔️ Exception on page {page}: {e}, retrying...")
            human_sleep(1, 6)
        else:
            print(f"⛔️ Failed to fetch page {page} for keyword: {keyword} after 3 attempts")
            break

        data = response.json()
        raw_json_per_keyword.append(data)
        videos = [item for item in data.get('data', {}).get('result', []) if item.get('type') == 'video']
        if not videos:
            print(f"⛔️ No videos found on page {page} for keyword: {keyword}")
            break
        all_videos.extend(videos)
        collected += len(videos)
        print(f"📄 Collected {len(videos)} videos from page {page}, total so far = {collected}")
        if collected >= target_count:
            break
        time.sleep(random.uniform(1, 6))

    all_raw_json[keyword] = raw_json_per_keyword

    # Fetch tags
    def fetch_tags_by_bvid(bvid):
        url = f"https://api.bilibili.com/x/tag/archive/tags?bvid={bvid}"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 412:
                print("🚨 [TAG] Hit anti-crawl (412), sleeping 60s...")
                time.sleep(60)
                return []
            if res.status_code == 200:
                tag_json = res.json()
                if tag_json.get('code') == -412:
                    print("🚫 [TAG] Code -412, anti-crawl triggered, sleeping 60s...")
                    time.sleep(60)
                    return []
                return [t['tag_name'] for t in tag_json.get('data', [])]
        except Exception as e:
            print(f"❌ [TAG] Exception while fetching tags: {e}")
        return []
    
    
    # Fetch user profile
    def fetch_uploader_profile(mid):
        url = f"https://api.bilibili.com/x/web-interface/card?mid={mid}"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 412:
                print("🚨 [PROFILE] Hit anti-crawl (412), sleeping 60s...")
                time.sleep(60)
                return default_profile()
            if res.status_code == 200:
                data = res.json().get("data", {}).get("card", {})
                if res.json().get("code") == -412:
                    print("🚫 [PROFILE] Code -412, anti-crawl triggered, sleeping 60s...")
                    time.sleep(60)
                    return default_profile()
                return {
                    "level": data.get("level_info", {}).get("current_level", 'N/A'),
                    "gender": data.get("sex", 'N/A')
                }
        except Exception as e:
            print(f"❌ [PROFILE] Exception while fetching profile: {e}")
        return default_profile()

    def default_profile():
        return {
            "level": 'N/A',
            "gender": 'N/A'
        }
    
    for i, video in enumerate(all_videos[:target_count]):
        print()  
        title = re.sub(r'<.*?>', '', video.get('title', 'N/A'))
        description = video.get('description', '')
        author = video.get('author', 'N/A')
        views = video.get('play', 'N/A')
        bvid = video.get('bvid', 'N/A')
        mid = video.get('mid', 'N/A')

        print(f"[{i+1}/{min(target_count, len(all_videos))}] ▶️ Processing video:")
        print(f"\033[92m🎬 Title: {title}\033[0m")

    
        video_detail_url = f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
        for attempt in range(3):
            try:
                detail_response = session.get(video_detail_url, headers=headers, timeout=10)
                if detail_response.status_code == 200:
                    break
            except:
                human_sleep(1, 3)
        else:
            print(f"⛔️ Failed to get video detail after 3 attempts: {bvid}")
            continue

        detail_data = detail_response.json().get('data', {})
        uploader_name = author
        followers = 'N/A'
        profile = fetch_uploader_profile(mid)
        uploader_gender = profile['gender']
        uploader_level = profile['level']
        

        likes = detail_data.get('stat', {}).get('like', 'N/A')
        shares = detail_data.get('stat', {}).get('share', 'N/A')
        favorites = detail_data.get('stat', {}).get('favorite', 'N/A')
        coins = detail_data.get('stat', {}).get('coin', 'N/A')
        comments = detail_data.get('stat', {}).get('reply', 'N/A')
        danmaku = detail_data.get('stat', {}).get('danmaku', 'N/A')
        pubdate_timestamp = detail_data.get('pubdate', 'N/A')
        pubdate = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(pubdate_timestamp)) if isinstance(pubdate_timestamp, int) else 'N/A'
        duration = detail_data.get('duration', 'N/A')
        duration_str = f"{duration // 60}m {duration % 60}s" if isinstance(duration, int) else 'N/A'
        send_time = detail_data.get('ctime', None)
        send_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(send_time)) if isinstance(send_time, int) else 'N/A'
        tags = ', '.join(fetch_tags_by_bvid(bvid)) or '⛔️ No tags available'


        # Fetch uploader follower count
        if mid != 'N/A':
            stat_url = f"https://api.bilibili.com/x/relation/stat?vmid={mid}"
            for attempt in range(3):
                try:
                    stat_resp = session.get(stat_url, headers=headers, timeout=10)
                    if stat_resp.status_code == 200:
                        followers_data = stat_resp.json().get("data", {})
                        followers = followers_data.get("follower", 'N/A')
                        break
                except:
                    human_sleep(1, 3)

        
        # Fetch comments (main + sub-reply, up to 200 total)
        comment_url = f"https://api.bilibili.com/x/v2/reply?type=1&oid={detail_data.get('aid')}&sort=0"
        page_num = 1
        comment_count = 0
        max_comment_count = 200
        if isinstance(comments, int) and comments <= 200:
            max_comment_count = comments
        while comment_count < max_comment_count:
            for attempt in range(3):
                try:
                    comment_resp = session.get(comment_url + f"&pn={page_num}", headers=headers, timeout=10)
                    if comment_resp.status_code == 200:
                        break
                except:
                    time.sleep(random.uniform(1, 3))
            else:
                print(f"⛔️ Failed to get comments page {page_num} for {bvid}")
                failed_comments_log.append({"keyword": keyword, "bvid": bvid, "page": page_num})
                break

            data = comment_resp.json().get("data", {})
            replies = data.get("replies", [])
            if not replies:
                break

            for r in replies:
                if comment_count >= max_comment_count:
                    break
                all_comments_combined.append([
                    keyword, bvid, title, uploader_name, pubdate, views, likes, shares, favorites, coins, comments, danmaku, tags,
                    "Top", r.get("member", {}).get("uname", "N/A"),
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.get("ctime", 0))),
                    r.get("content", {}).get("message", "")
                ])
                comment_count += 1

                # Sub replies
                for sc in r.get("replies", []) or []:
                    if comment_count >= max_comment_count:
                        break
                    all_comments_combined.append([
                        keyword, bvid, title, uploader_name, pubdate, views, likes, shares, favorites, coins, comments, danmaku, tags,
                        "↪️ Reply", sc.get("member", {}).get("uname", "N/A"),
                        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(sc.get("ctime", 0))),
                        sc.get("content", {}).get("message", "")
                    ])
                    comment_count += 1
            page_num += 1
            human_sleep(0.3, 1.2)

        print(f"💬 Collected {comment_count} total comments for video: {bvid}")

        print(f"👤 Uploader: {uploader_name} (Followers: {followers})")
        print(f"🆔 UID: {mid}")
        print(f"⚧️ Gender: {uploader_gender}")
        print(f"✅ Account Level: {uploader_level}")
        print(f"🗓️ Published Time: {pubdate}")
        print(f"🕐 Send Time: {send_time_str}")
        print(f"👀 Views: {views}")
        print(f"🩷 Likes: {likes}")
        print(f"🔄 Shares: {shares}")
        print(f"⭐ Favorites: {favorites}")
        print(f"💰 Coins: {coins}")
        print(f"💬 Comments (main+sub-reply): {comments}")
        print(f"📢 Danmaku: {danmaku}")
        print(f"⏱️ Duration: {duration_str}")
        print(f"🏷️ Tags: {tags}")
        print(f"🔗 Video Link: \033[94mhttps://www.bilibili.com/video/{bvid}\033[0m")
        print(f"📄 Description: {description}")
        print()

        all_video_records.append([
            keyword, title, uploader_name, views, likes, shares, favorites, coins, comments, danmaku,
            f"https://www.bilibili.com/video/{bvid}", mid, pubdate, send_time_str, duration_str, tags, description,
            uploader_gender, uploader_level
        ])




# ======================== Export and Checkpoint Save ========================
# ✅ Clean illegal characters for Excel compatibility
def clean_illegal_chars(value):
    if isinstance(value, str):
        return re.sub(r'[\x00-\x1F]', '', value)
    return value

# Export collected video metadata to Excel/CSV/JSON
df = pd.DataFrame(all_video_records, columns=[
    "Keyword", "Title", "Uploader", "Views", "Likes", "Shares", "Favorites", "Coins", "Comments", "Danmaku",
    "Link", "UID", "Published Date", "Send Time", "Duration", "Tags", "Description",
    "Gender", "Level"
])
df = df.applymap(clean_illegal_chars)
df.to_excel("bilibili_video_info.xlsx", index=False)
df.to_csv("bilibili_video_info.csv", index=False)
df.to_json("bilibili_video_info.json", force_ascii=False, orient='records', indent=4)
print("📄 Excel, CSV and JSON files saved for video metadata.")

# Export comments to Excel
if all_comments_combined:
    with pd.ExcelWriter("all_comments_combined.xlsx", engine='openpyxl') as writer:
        for kw in set([row[0] for row in all_comments_combined]):
            kw_comments = [row for row in all_comments_combined if row[0] == kw]
            comment_df = pd.DataFrame(kw_comments, columns=[
                 "Keyword", "BVID", "Title", "Uploader", "Published Date", "Views", "Likes", "Shares", "Favorites", "Coins",
                 "Comments", "Danmaku", "Tags", "Comment Type", "User", "Time", "Content"])
            comment_df = comment_df.applymap(clean_illegal_chars)
            sheet_name = kw[:31]
            comment_df.to_excel(writer, sheet_name=sheet_name, index=False)
            completed_comments.add(bvid)

    print("💬 Comment file saved: all_comments_combined.xlsx")
else:
    print("⛔️ No comments found, skipping comment Excel export.")

# Save checkpoint to resume later
with open("completed_keywords.json", "w", encoding="utf-8") as f:
    json.dump(list(completed_keywords.union({keyword})), f, ensure_ascii=False, indent=2)

with open("completed_comments.json", "w", encoding="utf-8") as f:
    json.dump(list(completed_comments), f, ensure_ascii=False, indent=2)

if failed_comments_log:
    with open("failed_comments.json", "w", encoding="utf-8") as f:
        json.dump(failed_comments_log, f, ensure_ascii=False, indent=2)
    print("🧱 Failed comment logs saved: failed_comments.json")

# Export raw JSON
with open("bilibili_raw_results.json", "a", encoding="utf-8") as f:
    json.dump(all_raw_json, f, ensure_ascii=False, indent=2)
print("📦 Raw JSON file saved: bilibili_raw_results.json")

# Merge video metadata and comments
if all_video_records and all_comments_combined:
    print("🔗 Merging video info and comments into single CSV file...")
    video_df = pd.DataFrame(all_video_records, columns=[
        "Keyword", "Title", "Uploader", "Views", "Likes", "Shares", "Favorites", "Coins", "Comments", "Danmaku",
        "Link", "UID", "Published Date", "Send Time", "Duration", "Tags", "Description",
        "Gender", "Level"
    ])
    video_df["BVID"] = video_df["Link"].apply(lambda x: x.split("/")[-1])

    comment_df = pd.DataFrame(all_comments_combined, columns=[
        "Keyword", "BVID", "Title", "Uploader", "Published Date", "Views", "Likes", "Shares", "Favorites", "Coins",
        "Comments", "Danmaku", "Tags", "Comment Type", "User", "Time", "Content"
    ])

    merged_df = pd.merge(comment_df, video_df, on="BVID", how="left", suffixes=("", "_video"))

    desired_columns = [
        "Keyword", "BVID", "Title", "Uploader", "UID", "Gender", "Level",
        "Published Date", "Send Time", "Views", "Likes", "Shares", "Favorites", "Coins", "Comments",
        "Danmaku", "Tags", "Description", "Link", "Comment Type", "User", "Time", "Content"
    ]
    merged_df = merged_df[desired_columns]
    merged_df = merged_df.applymap(clean_illegal_chars)
    merged_df.to_csv("bilibili_comment_with_video_info.csv", index=False, encoding="utf-8-sig")
    print("📎 Merged CSV saved: bilibili_comment_with_video_info.csv")
else:
    print("⛔️ Not enough data to merge comment and video info.")

# Save uploader profile data
with open("uploader_profile_raw.json", "a", encoding="utf-8") as f:
    json.dump(all_user_json, f, ensure_ascii=False, indent=2)
print("📄 JSON file saved: uploader_profile_raw.json")
