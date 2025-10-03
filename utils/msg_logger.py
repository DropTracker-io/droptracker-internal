from asyncio.log import logger
import time
import os
import threading
import json
from datetime import datetime
from typing import Dict, Any
from sqlalchemy.engine import Row




def make_json_safe(obj):
    """Recursively convert SQLAlchemy Rows and other non-serializable objects to serializable types."""
    if isinstance(obj, Row):
        return dict(obj._mapping)
    elif isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [make_json_safe(v) for v in obj]
    elif hasattr(obj, "isoformat"):  # e.g., datetime
        return obj.isoformat()
    else:
        return obj

class HighThroughputLogger:
    """
    Logger optimized for extremely high throughput scenarios with hundreds of operations per second.
    Uses buffering and batch writes to minimize I/O operations while maintaining data integrity.
    """
    
    def __init__(self, log_file_path: str, buffer_size: int = 1000, 
                 flush_interval: float = 1.0, max_file_size: int = 100 * 1024 * 1024):
        """
        Initialize the high throughput logger.
        
        Args:
            log_file_path: Path to the log file
            buffer_size: Maximum number of log entries to buffer before flushing
            flush_interval: Time in seconds between forced buffer flushes
            max_file_size: Maximum size of log file before rotation (default 100MB)
        """
        self.log_file_path = log_file_path
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.max_file_size = max_file_size
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        
        # Thread safety
        self.buffer_lock = threading.Lock()
        self.file_lock = threading.Lock()
        
        # Buffer for log entries
        self.log_buffer = []
        self.last_flush = time.time()
        
        # Start background flush timer
        self._start_flush_timer()
    
    def _start_flush_timer(self):
        """Start a background timer to periodically flush the log buffer"""
        def _timer_func():
            while True:
                time.sleep(self.flush_interval)
                self._flush_buffer_if_needed(force=True)
        
        flush_thread = threading.Thread(target=_timer_func, daemon=True)
        flush_thread.start()
    
    def _rotate_logs_if_needed(self):
        """Rotate log file if it exceeds the maximum size"""
        try:
            if os.path.exists(self.log_file_path) and os.path.getsize(self.log_file_path) > self.max_file_size:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                rotated_file = f"{self.log_file_path}.{timestamp}"
                os.rename(self.log_file_path, rotated_file)
                
                # Keep only the 10 most recent log files
                log_dir = os.path.dirname(self.log_file_path)
                base_name = os.path.basename(self.log_file_path)
                log_files = [f for f in os.listdir(log_dir) if f.startswith(base_name) and f != base_name]
                log_files.sort(reverse=True)
                
                for old_file in log_files[10:]:
                    os.remove(os.path.join(log_dir, old_file))
        except Exception as e:
            logger.error(f"Failed to rotate high-throughput logs: {e}", exc_info=True)
    
    def _flush_buffer_if_needed(self, force=False):
        """Flush the buffer if it's full or if forced"""
        current_time = time.time()
        should_flush = False
        buffer_to_flush = []
        
        with self.buffer_lock:
            if len(self.log_buffer) == 0:
                return
                
            if force and (current_time - self.last_flush) >= self.flush_interval:
                should_flush = True
            elif len(self.log_buffer) >= self.buffer_size:
                should_flush = True
                
            if should_flush:
                buffer_to_flush = self.log_buffer.copy()
                self.log_buffer = []
                self.last_flush = current_time
        
        if should_flush:
            self._write_buffer_to_file(buffer_to_flush)
    
    def _write_buffer_to_file(self, buffer):
        """Write a batch of log entries to file"""
        if not buffer:
            return
            
        try:
            with self.file_lock:
                self._rotate_logs_if_needed()
                
                with open(self.log_file_path, 'a') as f:
                    for entry in buffer:
                        safe_entry = make_json_safe(entry)
                        f.write(json.dumps(safe_entry) + "\n")
        except Exception as e:
            logger.error(f"Failed to write high-throughput log buffer to file: {e}", exc_info=True)
    
    async def log(self, 
                 event_type: str, 
                 data: Dict[str, Any],
                 console_output: bool = False) -> bool:
        """
        Asynchronous high-throughput logging method.
        
        Args:
            event_type: Type of event being logged
            data: Dictionary containing the data to log
            console_output: Whether to print to console (disabled by default)
            
        Returns:
            bool: True if the log entry was added to the buffer
        """
        # Only print to console if explicitly requested
        if console_output:
            print(f"[{event_type}] {data}")
        
        # Create log entry
        timestamp = datetime.now().isoformat()
        unix_timestamp = int(time.time())
        log_entry = {
            "timestamp": timestamp,
            "unix_timestamp": unix_timestamp,
            "event_type": event_type,
            "data": data
        }
        
        # Add to buffer
        with self.buffer_lock:
            self.log_buffer.append(log_entry)
        
        # Check if we need to flush in a non-blocking way
        if len(self.log_buffer) >= self.buffer_size:
            threading.Thread(
                target=self._flush_buffer_if_needed,
                daemon=True
            ).start()
        
        return True
    
    def log_sync(self, event_type: str, data: Dict[str, Any], console_output: bool = False) -> bool:
        """
        Synchronous version of the high-throughput logging method.
        
        Args:
            event_type: Type of event being logged
            data: Dictionary containing the data to log
            console_output: Whether to print to console (disabled by default)
            
        Returns:
            bool: True if the log entry was added to the buffer
        """
        # Only print to console if explicitly requested
        if console_output:
            print(f"[{event_type}] {data}")
        
        # Create log entry
        timestamp = datetime.now().isoformat()
        unix_timestamp = int(time.time())
        log_entry = {
            "timestamp": timestamp,
            "unix_timestamp": unix_timestamp,
            "event_type": event_type,
            "data": data
        }
        
        # Add to buffer
        with self.buffer_lock:
            self.log_buffer.append(log_entry)
            
            # Check if we need to flush
            if len(self.log_buffer) >= self.buffer_size:
                buffer_to_flush = self.log_buffer.copy()
                self.log_buffer = []
                self.last_flush = time.time()
                
                # Write in this thread since it's synchronous
                self._write_buffer_to_file(buffer_to_flush)
        
        return True
    
    def flush(self):
        """
        Force an immediate flush of the buffer.
        Useful when shutting down the application.
        """
        with self.buffer_lock:
            buffer_to_flush = self.log_buffer.copy()
            self.log_buffer = []
            self.last_flush = time.time()
        
        self._write_buffer_to_file(buffer_to_flush)

# Example usage:
# 
# await high_throughput_logger.log("message_processed", {"message_id": "123", "processing_time": 0.05})