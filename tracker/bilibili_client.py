import requests
import time
import random
import hashlib
import urllib.parse
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from config import config
from logger import logger

class BilibiliAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(config.BILI_API_HEADERS)
        self.session.cookies.update(config.BILI_COOKIES)
        self._last_request_time = 0
        self._request_interval = 1.2 
        self.wbi_keys = self._get_wbi_keys()
        logger.info("Bilibili API客户端初始化完成")
        print("Bilibili API客户端初始化完成")

    def _get_wbi_keys(self):
        try:
            resp = self.session.get(
                "https://api.bilibili.com/x/web-interface/nav",
                timeout=5
            )
            data = resp.json()
            img_key = data['data']['wbi_img']['img_url'].split('/')[-1].split('.')[0]
            sub_key = data['data']['wbi_img']['sub_url'].split('/')[-1].split('.')[0]
            return img_key + sub_key
        except Exception as e:
            logger.error(f"获取WBI密钥失败: {str(e)}")
            return "7cd084941338484aae1ad9425b84077c4932caff0ff746eab6f01bf08b70ac45"

    def _wbi_sign(self, params):
        params['wts'] = int(time.time())
        params = dict(sorted(params.items()))
        query = urllib.parse.urlencode(params)
        wbi_sign = hashlib.md5((query + self.wbi_keys).encode()).hexdigest()
        params['w_rid'] = wbi_sign
        return params

    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        base_delay = random.normalvariate(1.5, 0.3)
        freq_factor = 1.0
        if hasattr(self, '_recent_requests'):
            freq_factor = min(2.0, 1 + len(self._recent_requests)/10)
        mode = random.choice(["uniform", "expovariate", "burst"])
        
        if mode == "uniform": 
            delay = base_delay * random.uniform(0.8, 1.5)
        elif mode == "expovariate": 
            delay = base_delay * random.expovariate(1.2)
        else: 
            delay = base_delay * 0.2 if random.random() < 0.1 else base_delay
        if elapsed < 0.8: 
            delay += random.uniform(3.0, 8.0)
        if not hasattr(self, '_recent_requests'):
            self._recent_requests = []
        self._recent_requests.append(time.time())
        self._recent_requests = [t for t in self._recent_requests if time.time() - t < 60]
        
        time.sleep(delay)
        self._last_request_time = time.time()

    def search_videos(self, keyword, max_pages=1):
        return self.get_recent_videos(
            keyword, 
            max_results=20 * max_pages,  
            max_pages=max_pages
        )

    @lru_cache(maxsize=500)
    def get_video_details(self, bvid):
        return self._get_video_details(bvid, keyword="")

    def get_recent_videos(self, keyword, max_results=20, max_pages=1):
        self._rate_limit()
        results = []
        current_time = time.time()
        time_threshold = int(config.get_24h_threshold().timestamp())

        for page in range(1, max_pages + 1):
            try:
                signed_params = self._wbi_sign({
                    'search_type': 'video',
                    'keyword': keyword,
                    'order': 'pubdate',
                    'page': page,
                    'page_size': min(20, max_results - len(results))
                })

                response = self.session.get(
                    "https://api.bilibili.com/x/web-interface/wbi/search/type",
                    params=signed_params,
                    timeout=15
                )

                if response.status_code != 200:
                    logger.warning(f"搜索请求失败，状态码: {response.status_code}")
                    break

                data = response.json()
                if data.get('code') != 0:
                    logger.warning(f"API返回错误: {data.get('message')}")
                    break

                for video in data['data'].get('result', []):
                    pubdate = int(video.get('pubdate', 0))
                    if len(str(pubdate)) > 10: 
                        pubdate = pubdate // 1000

                    if pubdate >= time_threshold:
                        bvid = video.get('bvid')
                        if bvid:
                            detail = self._get_video_details(bvid, keyword)
                            if detail and len(results) < max_results:
                                results.append(detail)

                if len(results) >= max_results or len(data['data'].get('result', [])) < 20:
                    break

            except Exception as e:
                logger.error(f"搜索过程中出错: {str(e)}")
                break

        return results

    def _get_video_details(self, bvid, keyword):
        try:
            view_params = self._wbi_sign({'bvid': bvid})
            view_response = self.session.get(
                "https://api.bilibili.com/x/web-interface/wbi/view",
                params=view_params,
                timeout=10
            )
            
            if view_response.status_code != 200:
                return None

            view_data = view_response.json()
            if view_data.get('code') != 0:
                return None
            
            video_data = view_data.get('data', {})
            pub_ts = video_data.get('pubdate', 0)
            if len(str(pub_ts)) > 10:
                pub_ts = pub_ts // 1000

            beijing_time = datetime.fromtimestamp(pub_ts, timezone(timedelta(hours=8)))
            upload_date_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S')

            stat_params = self._wbi_sign({'bvid': bvid})
            stat_response = self.session.get(
                "https://api.bilibili.com/x/web-interface/wbi/archive/stat",
                params=stat_params,
                timeout=10
            )
            
            stat_data = stat_response.json().get('data', {}) if stat_response.status_code == 200 else {}

            stats = video_data.get('stat', {})
            return {
                'Keyword': keyword,
                'Title': video_data.get('title', f"Video_{bvid}"),
                'Link': f"https://www.bilibili.com/video/{bvid}",
                'bvid': bvid,
                'Uploader': video_data.get('owner', {}).get('name', ''),
                'Upload_Date': upload_date_str,
                'Views': stats.get('view', stat_data.get('view', 0)),
                'Likes': stats.get('like', stat_data.get('like', 0)),
                'Favorites': stats.get('favorite', stat_data.get('favorite', 0)),
                'Shares': stats.get('share', stat_data.get('share', 0)),
                'Comments': stats.get('reply', stat_data.get('reply', 0)),
                'Danmaku': stats.get('danmaku', stat_data.get('danmaku', 0)),
                'Coins': stats.get('coin', stat_data.get('coin', 0)),
                'Collect_Date': config.get_today_str()
            }

        except Exception as e:
            logger.error(f"获取视频详情出错: {str(e)}")
            return None

api_client = BilibiliAPIClient()
