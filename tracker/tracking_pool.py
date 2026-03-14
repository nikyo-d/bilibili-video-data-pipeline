import json
import os
import pandas as pd
from datetime import datetime, timedelta
from config import config
from logger import logger
import filelock

FINAL_COLUMNS = [
    "Keyword", "Title", "Link", "bvid", "Uploader", "Upload_Date",
    "Views", "Likes", "Favorites", "Shares", "Comments", "Danmaku", "Coins",
    "Collect_Date"
]

class TrackingPool:

    def __init__(self):
        self.pool_file = config.TRACKING_POOL_FILE
        self.removed_file = config.REMOVED_VIDEOS_FILE
        self.lock = filelock.FileLock(os.path.join(config.PROJECT_ROOT, "tracking.lock"))
        self._ensure_files_exist()

    def _ensure_files_exist(self):
        with self.lock:
            for filepath, default in [
                (self.pool_file, {"version": 2, "videos": {}, "last_updated": None}),
                (self.removed_file, {"version": 1, "removed": []})
            ]:
                if not os.path.exists(filepath):
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(default, f, ensure_ascii=False, indent=2)
                    logger.log_operation("INIT_FILE", file=os.path.basename(filepath))

    def _read_pool(self):
        with self.lock:
            with open(self.pool_file, 'r', encoding='utf-8') as f:
                return json.load(f)

    def _write_pool(self, data):
        with self.lock:
            data['last_updated'] = datetime.now().isoformat()
            temp_file = f"{self.pool_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(temp_file, self.pool_file)

    def _record_removed(self, bvid, reason, original_data):
        with self.lock:
            with open(self.removed_file, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                data['removed'].append({
                    "bvid": bvid,
                    "removed_at": datetime.now().isoformat(),
                    "reason": reason,
                    "last_stats": original_data['fields'],
                    "history_count": len(original_data['stats_history'])
                })
                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=2)

        logger.log_operation(
            "REMOVED_VIDEO",
            bvid=bvid,
            reason=reason,
            history=len(original_data['stats_history'])
        )

    def add_video(self, video_data):
        if not video_data or 'Link' not in video_data:
            return False, "invalid_data"

        bvid = self._extract_bvid(video_data['Link'])
        if not bvid:
            return False, "invalid_url"

        pool = self._read_pool()
        if bvid in pool['videos']:
            return False, "already_exists"

        pool['videos'][bvid] = {
            "fields": video_data,
            "first_seen": config.get_today_str(),
            "last_checked": config.get_today_str(),
            "stats_history": [{
                "date": config.get_today_str(),
                "stats": {k: v for k, v in video_data.items()
                          if k in ['Views', 'Likes', 'Favorites', 'Shares',
                                   'Comments', 'Danmaku', 'Coins']}
            }]
        }

        self._write_pool(pool)
        return True, "added"

    def _extract_bvid(self, url):
        parts = url.split('/')
        for part in parts:
            if part.startswith('BV') and len(part) == 12:
                return part
        return None

    def update_video_stats(self, bvid, new_stats):
        pool = self._read_pool()
        if bvid not in pool['videos']:
            return False, 0

        video = pool['videos'][bvid]
        last_stats = video['stats_history'][-1]['stats']

        changed = False
        for key in new_stats:
            old_val = last_stats.get(key, 0)
            new_val = new_stats[key]
            if abs(new_val - old_val) >= 1:
                changed = True
                break

        if changed:
            video['stats_history'].append({
                "date": config.get_today_str(),
                "stats": new_stats
            })
            video['last_checked'] = config.get_today_str()
            for k in new_stats:
                video['fields'][k] = new_stats[k]
            self._write_pool(pool)

        return changed, len(video['stats_history'])

    def get_all_tracked(self):
        return self._read_pool()['videos']

    def _export_video_list(self, video_list, prefix):
        df = pd.DataFrame(video_list)
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = "" if col in ["Keyword", "Title", "Link", "bvid", "Uploader", "Upload_Date"] else 0
        df = df[FINAL_COLUMNS]

        df.to_csv(f"{prefix}.csv", index=False, encoding='utf-8-sig')
        df.to_excel(f"{prefix}.xlsx", index=False)

        with open(f"{prefix}.json", 'w', encoding='utf-8') as f:
            json.dump(video_list, f, ensure_ascii=False, indent=2)

    def export_active_videos(self, days=3):
        today = config.get_today_str()
        output_dir = os.path.join(config.DAILY_SNAPSHOTS_DIR, today)
        os.makedirs(output_dir, exist_ok=True)

        videos = self.get_all_tracked()
        active_items = []

        for bvid, data in videos.items():
            history = data.get("stats_history", [])
            if len(history) < days:
                continue
            base = history[-days]["stats"]
            last = history[-1]["stats"]
            fields = data["fields"]
            if any(last.get(k, 0) > base.get(k, 0) for k in ["Likes", "Favorites", "Shares", "Comments", "Danmaku", "Coins"]):
                active_items.append(fields)

        prefix = os.path.join(output_dir, f"active_videos_{today}")
        self._export_video_list(active_items, prefix)
        logger.log_operation("EXPORT_ACTIVE", count=len(active_items))

    def remove_inactive_videos(self):
        pool = self._read_pool()
        today = config.get_today_str()
        today_date = datetime.strptime(today, "%Y-%m-%d").date()

        removed_items = []
        removed_ids = []

        for bvid, data in list(pool["videos"].items()):
            first_seen = datetime.strptime(data["first_seen"], "%Y-%m-%d").date()
            history = data.get("stats_history", [])
            days_tracked = (today_date - first_seen).days

            if days_tracked >= 3 and len(history) <= 2:
                self._record_removed(bvid, "short_lived", data)
                removed_items.append(data["fields"])
                removed_ids.append(bvid)
                del pool["videos"][bvid]
                continue

            if len(history) >= 3:
                base = history[-3]["stats"]
                last = history[-1]["stats"]
                if not any(last.get(f, 0) > base.get(f, 0) for f in ["Likes", "Favorites", "Shares", "Comments", "Danmaku", "Coins"]):
                    self._record_removed(bvid, "inactive_3days", data)
                    removed_items.append(data["fields"])
                    removed_ids.append(bvid)
                    del pool["videos"][bvid]

        if removed_items:
            self._write_pool(pool)
            output_dir = os.path.join(config.DAILY_SNAPSHOTS_DIR, today)
            os.makedirs(output_dir, exist_ok=True)
            prefix = os.path.join(output_dir, f"removed_videos_{today}")
            self._export_video_list(removed_items, prefix)
            logger.log_operation("REMOVE_INACTIVE", count=len(removed_items))

        return removed_ids

    def export_daily_videos(self):
        pool = self._read_pool()
        today = config.get_today_str()
        output_dir = os.path.join(config.DAILY_SNAPSHOTS_DIR, today)
        os.makedirs(output_dir, exist_ok=True)

        daily_items = []
        for bvid, data in pool['videos'].items():
            latest = data['stats_history'][-1]['stats']
            fields = data['fields']
            item = {
                "Keyword": fields.get("Keyword", ""),
                "Title": fields.get("Title", ""),
                "Link": fields.get("Link", f"https://www.bilibili.com/video/{bvid}"),
                "bvid": bvid,
                "Uploader": fields.get("Uploader", ""),
                "Upload_Date": fields.get("Upload_Date", ""),
                "Views": latest.get("Views", 0),
                "Likes": latest.get("Likes", 0),
                "Favorites": latest.get("Favorites", 0),
                "Shares": latest.get("Shares", 0),
                "Comments": latest.get("Comments", 0),
                "Danmaku": latest.get("Danmaku", 0),
                "Coins": latest.get("Coins", 0),
                "Collect_Date": today
            }
            daily_items.append(item)

        prefix = os.path.join(output_dir, f"daily_videos_{today}")
        self._export_video_list(daily_items, prefix)
        logger.info(f"Generated daily_videos: {len(daily_items)} videos (columns: {len(FINAL_COLUMNS)})")
        return daily_items

    def export_daily_summary(self):
        today = config.get_today_str()
        output_dir = os.path.join(config.DAILY_SNAPSHOTS_DIR, today)
        os.makedirs(output_dir, exist_ok=True)

        tracked_count = len(self.get_all_tracked())
        removed_path = os.path.join(output_dir, f"removed_videos_{today}.json")
        active_path = os.path.join(output_dir, f"active_videos_{today}.json")
        new_json_path = os.path.join(output_dir, f"new_videos_{today}.json")

        removed_count = 0
        active_count = 0
        new_video_count = 0

        if os.path.exists(removed_path):
            with open(removed_path, "r", encoding="utf-8") as f:
                removed_count = len(json.load(f))

        if os.path.exists(active_path):
            with open(active_path, "r", encoding="utf-8") as f:
                active_count = len(json.load(f))

        if os.path.exists(new_json_path):
            with open(new_json_path, "r", encoding="utf-8") as f:
                new_video_count = len(json.load(f))

        daily_fetched_count = tracked_count + removed_count

        summary = f"""📊 Bilibili Daily Summary - {today}

📥 Daily videos fetched:         {daily_fetched_count}
🆕 Newly added videos today:     {new_video_count}
🧊 Removed inactive videos:      {removed_count}
📈 Active videos today:          {active_count}
🎯 Final tracked videos:         {tracked_count}
"""

        def normalize_keyword(keyword):
            return " ".join(sorted(set(keyword.lower().split())))

        original_keywords = config.SEARCH_TERMS
        normalized_set = {normalize_keyword(k): k for k in original_keywords}

        matched_keywords = {
            normalize_keyword(data["fields"].get("Keyword", ""))
            for data in self.get_all_tracked().values()
        }

        summary += f"\n🔤 Total normalized keywords (matched to original set): {len(matched_keywords)} / {len(normalized_set)}\n"

        with open(os.path.join(output_dir, f"summary_{today}.txt"), "w", encoding="utf-8") as f:
            f.write(summary)

        logger.send_slack_message(summary)
        logger.log_operation("EXPORT_SUMMARY", tracked=tracked_count, removed=removed_count,
                             active=active_count, daily=daily_fetched_count, new=new_video_count)

        print(summary)

    def run_daily_operations(self):
        self.export_active_videos()
        self.export_daily_videos()
        self.export_daily_summary()


tracker = TrackingPool()
