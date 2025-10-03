from db import XenforoSession, xenforo_engine
import time
import json
import redis
import threading
from dotenv import load_dotenv
import os
from utils.redis import redis_client
from sqlalchemy import text
from datetime import datetime

load_dotenv()

BATCH_SIZE = 100
BATCH_TIMEOUT = 1  # seconds


class AppLogger:
    _worker_started = False
    _worker_lock = threading.Lock()

    def __init__(self):
        self.engine = xenforo_engine
        self.redis_client = redis_client
        # Remove psycopg2 connection, use session instead
        with AppLogger._worker_lock:
            if not AppLogger._worker_started:
                t = threading.Thread(target=self._log_worker, daemon=True)
                t.start()
                AppLogger._worker_started = True

    def log(self, log_type, data, app_name, description):
        log_entry = {
            "type": log_type,
            "data": data,
            "app_name": app_name,
            "description": description
        }
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{time}] [{log_type}]: {data} {description} ({app_name})")
        self.redis_client.rpush("log_queue", json.dumps(log_entry))

    def _batch_insert_logs(self, logs):
        # # Create a new session for this batch
        # session = XenforoSession()
        # try:
        #     raw_sql = text("""
        #     INSERT INTO dt_app_log (date, log_type, log_data, app_name, description)
        #     VALUES (:log_date, :log_type, :log_data, :app_name, :description)
        #     """)
        #     for log in logs:
        #         session.execute(
        #             raw_sql,
        #             {
        #                 "log_date": time.strftime('%Y-%m-%d %H:%M:%S'),
        #                 "log_type": log['type'],
        #                 "log_data": log['data'],
        #                 "app_name": log['app_name'],
        #                 "description": log['description']
        #             }
        #         )
        #     session.commit()
        # except Exception as e:
        #     session.rollback()
        #     print(f"[AppLogger] Error during batch insert: {e}")
        # finally:
        #     session.close()
        pass

    def _log_worker(self):
        while True:
            logs = []
            start_time = time.time()
            while len(logs) < BATCH_SIZE and (time.time() - start_time) < BATCH_TIMEOUT:
                log_json = self.redis_client.lpop("log_queue")
                if log_json:
                    logs.append(json.loads(log_json))
                else:
                    time.sleep(0.01)  # avoid busy-waiting
            if logs:
                self._batch_insert_logs(logs)

