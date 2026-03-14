import logging
import os
import requests
from config import config

class BiliLogger:
    def __init__(self):
        self.logger = logging.getLogger('bili_tracker')
        self._setup_logging()

    def _setup_logging(self):
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        os.makedirs(config.LOGS_DIR, exist_ok=True)
        log_file = os.path.join(config.LOGS_DIR, f"track_{config.get_today_str()}.log")

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def log_operation(self, operation, **kwargs):
        message = f"{operation.upper()}"
        if kwargs:
            message += " | " + " ".join(f"{k}={v}" for k, v in kwargs.items())
        self.logger.info(message)

    def info(self, msg, *args, **kwargs): self.logger.info(msg, *args, **kwargs)
    def debug(self, msg, *args, **kwargs): self.logger.debug(msg, *args, **kwargs)
    def warning(self, msg, *args, **kwargs): self.logger.warning(msg, *args, **kwargs)
    def error(self, msg, *args, **kwargs): self.logger.error(msg, *args, **kwargs)

    def send_slack_message(self, text: str):
        slack_url = getattr(config, "SLACK_WEBHOOK_URL", None)
        if not slack_url:
            self.warning("SLACK | SLACK_WEBHOOK_URL not configured")
            return

        try:
            response = requests.post(slack_url, json={"text": text})
            if response.status_code == 200:
                self.log_operation("SLACK", status="success")
            else:
                self.log_operation("SLACK", status="fail", code=response.status_code)
        except Exception as e:
            self.error(f"SLACK | Exception: {e}")

logger = BiliLogger()
