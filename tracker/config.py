import os
from datetime import datetime, timedelta, timezone

class Config:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    DAILY_SNAPSHOTS_DIR = os.path.join(PROJECT_ROOT, "daily_snapshots")
    SLACK_WEBHOOK_URL = ""
    LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
    LEGACY_DATA_DIR = os.path.join(PROJECT_ROOT, "legacy_data")
    TRACKING_POOL_FILE = os.path.join(PROJECT_ROOT, "tracking_pool.json")
    REMOVED_VIDEOS_FILE = os.path.join(PROJECT_ROOT, "removed_videos.json")
    INACTIVITY_THRESHOLD = 3
    NEW_VIDEO_WINDOW = 1
    PLATFORM_DELETED_MARKER = "deleted"
    USER_DELETED_MARKER = "user_removed"
    MAX_SEARCH_PAGES = 10
    REQUEST_DELAY = (1.0, 3.0)

    BILI_API_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.bilibili.com/",
        "Origin": "https://www.bilibili.com",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "X-Requested-With": "XMLHttpRequest"
    }

    BILI_COOKIES = {
        "SESSDATA": "",
        "bili_jct": "",
        "DedeUserID": ""
    }

    TOPIC_KEYWORDS = [
        "Belt and Road Initiative (BRI)", "One Belt One Road (OBOR)", "Maritime Silk Road",
        "Silk Road Economic Belt", "Chinese investment", "Chinese infrastructure projects",
        "Debt diplomacy", "Economic cooperation", "Strategic partnerships",
        "一带一路倡议 (BRI)", "一带一路 (OBOR)", "海上丝绸之路", "丝绸之路经济带",
        "中国投资", "中国基础设施项目", "债务外交", "经济合作", "战略伙伴关系",
        "一帶一路倡議", "一帶一路", "海上絲路", "絲路經濟帶",
        "中國投資", "中國基礎建設項目", "債務外交", "經濟合作", "戰略夥伴關係"
    ]

    LOCATION_KEYWORDS = [
        "Papua New Guinea (PNG)", "Solomon Islands (SI)", "Vanuatu", "Fiji",
        "巴布亚新几内亚 (PNG)", "所罗门群岛 (SI)", "瓦努阿图", "斐济",
        "巴布亞紐幾內亞 (PNG)", "所羅門群島 (SI)", "瓦努阿圖", "斐濟"
    ]

    @property
    def SEARCH_TERMS(self):
        return (
            self.TOPIC_KEYWORDS
            + [f"{t} {l}" for t in self.TOPIC_KEYWORDS
               for l in self.LOCATION_KEYWORDS]
        )

    @staticmethod
    def get_today_str():
        cst = timezone(timedelta(hours=8))
        return datetime.now(cst).strftime("%Y-%m-%d")

    @staticmethod
    def get_beijing_time():
        return datetime.now(timezone(timedelta(hours=8)))

    @staticmethod
    def get_beijing_time_str():
        return Config.get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_hours_since(timestamp_str):
        try:
            past = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            return (Config.get_beijing_time() - past).total_seconds() / 3600
        except:
            return float('inf')

    @staticmethod
    def get_24h_threshold():
        return Config.get_beijing_time() - timedelta(hours=24)

    @staticmethod
    def setup_directories():
        os.makedirs(Config.DAILY_SNAPSHOTS_DIR, exist_ok=True)
        os.makedirs(Config.LOGS_DIR, exist_ok=True)
        os.makedirs(Config.LEGACY_DATA_DIR, exist_ok=True)

    def __init__(self):
        self._validate_cookies()
        self.setup_directories()

    def _validate_cookies(self):
        required = ["SESSDATA", "bili_jct", "DedeUserID"]
        for key in required:
            if not self.BILI_COOKIES.get(key):
                raise ValueError(f"Missing required cookie: {key}")


config = Config()
Config.setup_directories()
