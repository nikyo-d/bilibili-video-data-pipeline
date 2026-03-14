#!/usr/bin/env python3
import argparse
import sys
import os
import json
import time
from datetime import datetime
from config import config
from logger import logger
from bilibili_client import api_client
from tracking_pool import tracker
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import random

class BiliTracker:

    def __init__(self):
        self.today = config.get_today_str()
        self.snapshot_dir = os.path.join(config.DAILY_SNAPSHOTS_DIR, self.today)
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.tracker = tracker

    def _save_snapshot(self, data, name):
        prefix = os.path.join(self.snapshot_dir, f"{name}_{self.today}")
        with open(f"{prefix}.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if isinstance(data, (list, dict)) and len(data) > 0:
            df = pd.DataFrame(data.values() if isinstance(data, dict) else data)

            if 'link' in df.columns and 'bvid' not in df.columns:
                df['bvid'] = df['link'].apply(lambda x: x.split('/')[-1] if isinstance(x, str) and '/video/' in x else '')

            def normalize_date(row):
                dt = pd.NaT
                if pd.notna(row.get('upload_date')):
                    try:
                        dt = pd.to_datetime(row['upload_date'], errors='coerce', dayfirst=True)
                    except:
                        pass
                if pd.isna(dt) and 'pubdate' in row and pd.notna(row['pubdate']):
                    try:
                        dt = pd.to_datetime(int(row['pubdate']), unit='s', utc=True).tz_convert('Asia/Shanghai')
                    except:
                        dt = pd.NaT
                if pd.isna(dt):
                    return ""
                return dt.strftime("%Y-%m-%d %H:%M:%S")

            if 'upload_date' in df.columns or 'pubdate' in df.columns:
                df['upload_date'] = df.apply(normalize_date, axis=1)

            df.to_csv(f"{prefix}.csv", index=False, encoding='utf-8-sig')
            df.to_excel(f"{prefix}.xlsx", index=False)


    def crawl_new_videos(self):
        new_videos = []
        all_keywords = config.SEARCH_TERMS
        total_keywords = len(all_keywords)
        all_fetched = []

        for i, keyword in enumerate(tqdm(all_keywords, desc="🔍 Searching keywords")):
            logger.log_operation("SEARCH", index=i + 1, total=total_keywords, keyword=keyword)
            videos = api_client.search_videos(keyword, max_pages=1)
            if not videos:
                continue

            bvid_to_video = {v.get('bvid'): v for v in videos if v.get('bvid')}
            all_fetched.extend(bvid_to_video.values())

            with ThreadPoolExecutor(max_workers=8) as executor:
                future_map = {
                    executor.submit(api_client.get_video_details, bvid): bvid
                    for bvid in bvid_to_video
                }

                for future in as_completed(future_map):
                    bvid = future_map[future]
                    try:
                        details = future.result()
                        if not details:
                            continue
                        details['Keyword'] = keyword
                        success, reason = self.tracker.add_video(details)
                        if success:
                            new_videos.append(details)
                            logger.debug(f"New video: {bvid} ({details['Title']})")
                        elif reason != "already_exists":
                            logger.warning(f"Failed to add {bvid}: {reason}")
                    except Exception as e:
                        logger.error(f"Error processing {bvid}: {str(e)}", exc_info=True)

            time.sleep(random.uniform(0.2, 0.5))

        self.all_fetched_videos = all_fetched
        if new_videos:
            self._save_snapshot(new_videos, "new_videos")
            logger.log_operation("ADD_VIDEOS", count=len(new_videos))

        return new_videos

    def update_existing_videos(self):
        changed_videos = {}
        all_videos = self.tracker.get_all_tracked()
        total = len(all_videos)
        logger.log_operation("UPDATE_PHASE", total=total)

        for bvid, data in tqdm(all_videos.items(), desc="📊 Updating existing videos", unit="video"):
            logger.debug(f"Checking {bvid[:8]}...")
            current_stats = api_client.get_video_details(bvid)
            if not current_stats:
                continue

            stats_subset = {
                k: current_stats[k]
                for k in ['Views', 'Likes', 'Favorites', 'Shares', 'Comments', 'Danmaku', 'Coins']
            }

            changed, _ = self.tracker.update_video_stats(bvid, stats_subset)
            if changed:
                old_stats = data['stats_history'][-2]['stats'] if len(data['stats_history']) >= 2 else {}
                fields = data.get('fields', {})
                changed_videos[bvid] = {
                    'old': old_stats,
                    'new': stats_subset,
                    'Keyword': fields.get('Keyword') or current_stats.get('Keyword', ''),
                    'Title': fields.get('Title') or current_stats.get('Title', ''),
                    'Collect_Date': self.today
                }

        if changed_videos:
            self._save_snapshot(changed_videos, "changed_videos")
            logger.log_operation("UPDATED_VIDEOS", count=len(changed_videos))

        return changed_videos

    def clean_inactive_videos(self):
        removed = self.tracker.remove_inactive_videos()
        if removed:
            self._save_snapshot(removed, "removed_videos")
        return removed

    def export_all_reports(self, new_videos, changed_videos, removed_videos):
        try:
            with open(config.TRACKING_POOL_FILE, "r", encoding="utf-8") as f:
                tracking_data = json.load(f)
            self._save_snapshot(tracking_data, "tracking_pool")
        except Exception as e:
            logger.error(f"⚠️ Failed to backup tracking_pool.json: {e}")

        if changed_videos:
            engagement_rows = []
            for bvid, item in changed_videos.items():
                row = {
                    "Keyword": item.get("Keyword", ""),
                    "Title": item.get("Title", ""),
                    "bvid": bvid,
                    "Collect_Date": item.get("Collect_Date", self.today)
                }
                for field in ["Views", "Likes", "Shares", "Favorites", "Comments", "Danmaku", "Coins"]:
                    new_val = item.get("new", {}).get(field, 0)
                    old_val = item.get("old", {}).get(field, 0)
                    row[f"{field}_Inc"] = max(0, new_val - old_val)
                engagement_rows.append(row)
            self._save_snapshot(engagement_rows, f"bilibili_engagement")

        if new_videos:
            self._save_snapshot(new_videos, "new_videos")

        if removed_videos:
            self._save_snapshot(removed_videos, "removed_videos")

        pool = self.tracker.get_all_tracked()
        all_rows = []
        for bvid, item in pool.items():
            fields = item.get("fields", {})
            fields.update({"bvid": bvid, "Collect_Date": self.today})
            all_rows.append(fields)
        self._save_snapshot(all_rows, "daily_videos")

        if hasattr(self, "all_fetched_videos"):
            self._save_snapshot(self.all_fetched_videos, "fetched_videos")

    def run(self, test_mode=False):
        start_time = time.time()
        logger.log_operation("START", date=self.today, test_mode=test_mode)
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_new = executor.submit(self.crawl_new_videos)
                future_update = executor.submit(self.update_existing_videos)
                new_videos = future_new.result() or []
                updated_videos = future_update.result() or []

            removed_videos = self.clean_inactive_videos()
            self.export_all_reports(new_videos, updated_videos, removed_videos)

            logger.log_operation(
                "COMPLETE",
                new=len(new_videos),
                updated=len(updated_videos),
                removed=len(removed_videos),
                runtime=f"{time.time() - start_time:.2f}s"
            )
            return True

        except Exception as e:
            logger.log_operation("FATAL", error=str(e), test_mode=test_mode)
            logger.error("Fatal error occurred", exc_info=True)
            return False

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Dry run mode")
    parser.add_argument("--force", action="store_true", help="Ignore safety checks")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    pid_file = os.path.join(config.PROJECT_ROOT, "running.pid")  

    if not args.force:
        if os.path.exists(pid_file):
            with open(pid_file) as f:
                pid = f.read().strip()
            logger.error(f"Another instance is running (PID: {pid})")
            sys.exit(1)
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))

    try:
        tracker_runner = BiliTracker()
        success = tracker_runner.run(test_mode=args.test)
        if success:
            tracker_runner.tracker.run_daily_operations()
        sys.exit(0 if success else 1)
    finally:
        if not args.force and os.path.exists(pid_file):
            os.remove(pid_file)
