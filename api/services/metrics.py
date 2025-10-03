import time
import threading
from collections import defaultdict, deque
from typing import DefaultDict, Deque, Dict, Tuple, Optional
from utils.redis import RedisClient


# Metrics tracking
class MetricsTracker:
    def __init__(self, window_minutes=60, use_redis: bool = True, redis_namespace: str = "metrics"):
        self.window_minutes = window_minutes
        # Stores (timestamp, type, success, app)
        self.requests: Deque[Tuple[float, str, bool, str]] = deque()
        self.lock = threading.Lock()

        # Counters for total metrics
        self.total_requests: int = 0
        self.total_by_type: DefaultDict[str, int] = defaultdict(int)
        self.total_success: int = 0
        self.total_failure: int = 0

        # Per-app totals
        self.total_by_app: DefaultDict[str, int] = defaultdict(int)
        self.total_success_by_app: DefaultDict[str, int] = defaultdict(int)
        self.total_failure_by_app: DefaultDict[str, int] = defaultdict(int)
        # Per-type per-app totals
        self.total_by_type_and_app: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Redis support
        self.redis_namespace = redis_namespace
        self.redis: Optional[RedisClient] = None
        if use_redis:
            try:
                self.redis = RedisClient()
            except Exception:
                self.redis = None

    def record_request(self, request_type, success, app: str = "unknown"):
        """Record a request with its timestamp, type, success status, and app label"""
        now = time.time()

        with self.lock:
            # Add the new request
            self.requests.append((now, request_type, success, app))

            # Update total counters
            self.total_requests += 1
            self.total_by_type[request_type] += 1
            self.total_by_app[app] += 1
            self.total_by_type_and_app[app][request_type] += 1
            if success:
                self.total_success += 1
                self.total_success_by_app[app] += 1
            else:
                self.total_failure += 1
                self.total_failure_by_app[app] += 1

            # Clean up old requests outside the window
            self._clean_old_requests(now)

            # Also persist to Redis (aggregated across processes)
            if self.redis and getattr(self.redis, "client", None):
                try:
                    minute_bucket = int(now // 60)
                    ttl_seconds = int(self.window_minutes * 60 + 120)
                    ns = self.redis_namespace
                    is_success = 1 if success else 0
                    pipe = self.redis.client.pipeline()

                    # All-time counters
                    pipe.incrby(f"{ns}:all:total", 1)
                    pipe.hincrby(f"{ns}:all:types", request_type, 1)
                    pipe.hincrby(f"{ns}:all:apps", app, 1)
                    pipe.hincrby(f"{ns}:all:app:{app}:types", request_type, 1)
                    if success:
                        pipe.incrby(f"{ns}:all:success", 1)
                        pipe.hincrby(f"{ns}:all:success_by_app", app, 1)
                    else:
                        pipe.incrby(f"{ns}:all:failure", 1)
                        pipe.hincrby(f"{ns}:all:failure_by_app", app, 1)

                    # Window (per-minute) counters
                    base = f"{ns}:win:{minute_bucket}"
                    pipe.incrby(f"{base}:total", 1)
                    if success:
                        pipe.incrby(f"{base}:success", 1)
                    else:
                        pipe.incrby(f"{base}:failure", 1)
                    pipe.hincrby(f"{base}:types", request_type, 1)
                    pipe.hincrby(f"{base}:apps", app, 1)
                    pipe.hincrby(f"{base}:app:{app}:types", request_type, 1)
                    if success:
                        pipe.hincrby(f"{base}:success_by_app", app, 1)
                    else:
                        pipe.hincrby(f"{base}:failure_by_app", app, 1)

                    # Ensure expiry for window keys
                    for k in [f"{base}:total", f"{base}:success", f"{base}:failure",
                              f"{base}:types", f"{base}:apps",
                              f"{base}:success_by_app", f"{base}:failure_by_app",
                              f"{base}:app:{app}:types"]:
                        pipe.expire(k, ttl_seconds)

                    pipe.execute()
                except Exception:
                    # Do not fail app logic on Redis errors
                    pass
    
    def _clean_old_requests(self, current_time):
        """Remove requests older than the window"""
        cutoff = current_time - (self.window_minutes * 60)
        
        while self.requests and self.requests[0][0] < cutoff:
            self.requests.popleft()
    
    def get_requests_per_minute(self):
        """Calculate requests per minute in the current window"""
        if not self.requests:
            return 0
        
        now = time.time()
        self._clean_old_requests(now)
        
        # Calculate time span in minutes
        if not self.requests:
            return 0
            
        oldest = self.requests[0][0]
        time_span = (now - oldest) / 60  # convert to minutes
        
        # Avoid division by zero
        if time_span < 0.01:
            return len(self.requests) * 60  # extrapolate to per minute
            
        return len(self.requests) / time_span
    
    def get_stats(self):
        """Get current statistics"""
        now = time.time()
        self._clean_old_requests(now)

        # If Redis is available, aggregate from Redis for cross-process stats
        if self.redis and getattr(self.redis, "client", None):
            try:
                ns = self.redis_namespace
                minute_now = int(now // 60)
                minutes = [minute_now - i for i in range(self.window_minutes)]

                # Helper to sum integer keys safely
                def _sum_int_keys(keys):
                    total = 0
                    for k in keys:
                        val = self.redis.client.get(k)
                        if val is not None:
                            try:
                                total += int(val)
                            except Exception:
                                continue
                    return total

                # Aggregate window totals
                win_total = _sum_int_keys([f"{ns}:win:{m}:total" for m in minutes])
                win_success = _sum_int_keys([f"{ns}:win:{m}:success" for m in minutes])
                win_failure = _sum_int_keys([f"{ns}:win:{m}:failure" for m in minutes])

                # Aggregate window hashes
                def _merge_hashes(keys):
                    acc: DefaultDict[str, int] = defaultdict(int)
                    for k in keys:
                        data = self.redis.client.hgetall(k)
                        if data:
                            for bkey, bval in data.items():
                                try:
                                    field = bkey.decode("utf-8") if isinstance(bkey, (bytes, bytearray)) else str(bkey)
                                    acc[field] += int(bval)
                                except Exception:
                                    continue
                    return dict(acc)

                win_by_type = _merge_hashes([f"{ns}:win:{m}:types" for m in minutes])
                win_by_app = _merge_hashes([f"{ns}:win:{m}:apps" for m in minutes])
                win_success_by_app = _merge_hashes([f"{ns}:win:{m}:success_by_app" for m in minutes])
                win_failure_by_app = _merge_hashes([f"{ns}:win:{m}:failure_by_app" for m in minutes])

                # For per-type per-app, build from minute app list
                requests_by_type_and_app: Dict[str, Dict[str, int]] = {}
                for app in win_by_app.keys():
                    app_types = _merge_hashes([f"{ns}:win:{m}:app:{app}:types" for m in minutes])
                    if app_types:
                        requests_by_type_and_app[app] = app_types

                # All-time aggregates
                def _get_int(key):
                    val = self.redis.client.get(key)
                    if val is None:
                        return 0
                    try:
                        return int(val)
                    except Exception:
                        return 0

                all_total = _get_int(f"{ns}:all:total")
                all_success = _get_int(f"{ns}:all:success")
                all_failure = _get_int(f"{ns}:all:failure")
                all_by_type = self.redis.client.hgetall(f"{ns}:all:types")
                all_by_app = self.redis.client.hgetall(f"{ns}:all:apps")
                all_success_by_app_h = self.redis.client.hgetall(f"{ns}:all:success_by_app")
                all_failure_by_app_h = self.redis.client.hgetall(f"{ns}:all:failure_by_app")

                def _decode_hash(h):
                    out: Dict[str, int] = {}
                    if not h:
                        return out
                    for k, v in h.items():
                        try:
                            key = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
                            out[key] = int(v)
                        except Exception:
                            continue
                    return out

                all_by_type_d = _decode_hash(all_by_type)
                all_by_app_d = _decode_hash(all_by_app)
                all_success_by_app_d = _decode_hash(all_success_by_app_h)
                all_failure_by_app_d = _decode_hash(all_failure_by_app_h)

                # Per-type per-app (all time)
                all_by_type_and_app: Dict[str, Dict[str, int]] = {}
                for app in all_by_app_d.keys():
                    h = self.redis.client.hgetall(f"{ns}:all:app:{app}:types")
                    all_by_type_and_app[app] = _decode_hash(h)

                # Requests per minute across window
                requests_per_minute = win_total / max(1, self.window_minutes)

                return {
                    "current_window": {
                        "requests_total": win_total,
                        "requests_per_minute": requests_per_minute,
                        "requests_by_type": win_by_type,
                        "requests_by_app": win_by_app,
                        "requests_by_type_and_app": requests_by_type_and_app,
                        "success_count": win_success,
                        "failure_count": win_failure,
                        "success_by_app": win_success_by_app,
                        "failure_by_app": win_failure_by_app,
                        "success_rate": (win_success / win_total * 100) if win_total else 0
                    },
                    "all_time": {
                        "requests_total": all_total,
                        "requests_by_type": all_by_type_d,
                        "requests_by_app": all_by_app_d,
                        "requests_by_type_and_app": all_by_type_and_app,
                        "success_count": all_success,
                        "failure_count": all_failure,
                        "success_by_app": all_success_by_app_d,
                        "failure_by_app": all_failure_by_app_d,
                        "success_rate": (all_success / all_total * 100) if all_total else 0
                    }
                }
            except Exception:
                # Fall back to in-memory if Redis aggregation fails
                pass
        
        # Count by type in current window
        types_count: DefaultDict[str, int] = defaultdict(int)
        success_count = 0
        failure_count = 0

        # Per-app counters (current window)
        by_app_count: DefaultDict[str, int] = defaultdict(int)
        success_by_app: DefaultDict[str, int] = defaultdict(int)
        failure_by_app: DefaultDict[str, int] = defaultdict(int)
        by_type_and_app: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))

        for _, req_type, success, app in self.requests:
            types_count[req_type] += 1
            by_app_count[app] += 1
            by_type_and_app[app][req_type] += 1
            if success:
                success_count += 1
                success_by_app[app] += 1
            else:
                failure_count += 1
                failure_by_app[app] += 1

        return {
            "current_window": {
                "requests_total": len(self.requests),
                "requests_per_minute": self.get_requests_per_minute(),
                "requests_by_type": dict(types_count),
                "requests_by_app": dict(by_app_count),
                "requests_by_type_and_app": {app: dict(types) for app, types in by_type_and_app.items()},
                "success_count": success_count,
                "failure_count": failure_count,
                "success_by_app": dict(success_by_app),
                "failure_by_app": dict(failure_by_app),
                "success_rate": (success_count / len(self.requests) * 100) if self.requests else 0
            },
            "all_time": {
                "requests_total": self.total_requests,
                "requests_by_type": dict(self.total_by_type),
                "requests_by_app": dict(self.total_by_app),
                "requests_by_type_and_app": {app: dict(types) for app, types in self.total_by_type_and_app.items()},
                "success_count": self.total_success,
                "failure_count": self.total_failure,
                "success_by_app": dict(self.total_success_by_app),
                "failure_by_app": dict(self.total_failure_by_app),
                "success_rate": (self.total_success / self.total_requests * 100) if self.total_requests else 0
            }
        }
