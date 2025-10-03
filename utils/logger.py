import aiohttp
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any
import logging
import threading

# Configure standard logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

base_url = "https://www.droptracker.io/"
LOG_FILE_PATH = "/store/droptracker/disc/data/logs/app_logs.json"
LOG_ROTATION_SIZE = 10 * 1024 * 1024  # 10MB

class LoggerClient:
    def __init__(self, token: str = None):
        """
        Initialize the logger client.
        
        Args:
            token: Authentication token for logging (optional, will use env var if not provided)
        """
        self.base_url = base_url.rstrip('/')
        self.token = token or os.getenv('LOGGER_TOKEN')
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
        
        # Lock for thread-safe file operations
        self.file_lock = threading.Lock()
    
    def _rotate_logs_if_needed(self):
        """Rotate log file if it exceeds the maximum size"""
        try:
            if os.path.exists(LOG_FILE_PATH) and os.path.getsize(LOG_FILE_PATH) > LOG_ROTATION_SIZE:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                rotated_file = f"{LOG_FILE_PATH}.{timestamp}"
                os.rename(LOG_FILE_PATH, rotated_file)
                logger.info(f"Rotated log file to {rotated_file}")
                
                # Keep only the 5 most recent log files
                log_dir = os.path.dirname(LOG_FILE_PATH)
                base_name = os.path.basename(LOG_FILE_PATH)
                log_files = [f for f in os.listdir(log_dir) if f.startswith(base_name) and f != base_name]
                log_files.sort(reverse=True)
                
                for old_file in log_files[5:]:
                    os.remove(os.path.join(log_dir, old_file))
                    logger.info(f"Deleted old log file: {old_file}")
        except Exception as e:
            logger.error(f"Failed to rotate logs: {e}", exc_info=True)
    
    def _write_to_file(self, log_entry: Dict[str, Any]):
        """Write a log entry to the file"""
        try:
            # Use a lock to ensure thread safety
            with self.file_lock:
                # Check if rotation is needed
                self._rotate_logs_if_needed()
                
                # Write the log entry
                with open(LOG_FILE_PATH, 'a') as f:
                    f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            logger.error(f"Failed to write log to file: {e}", exc_info=True)
    
    async def log(self, 
                 log_type: str, 
                 message: str, 
                 context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Send a log entry to the server and write to local file.
        
        Args:
            log_type: Type of log (error, access, or cron)
            message: The log message
            context: Optional dictionary of additional context
            
        Returns:
            bool: True if logging was successful
        """
        # Print to console
        print(f"[{log_type}] {message}")
        
        # Create log entry
        timestamp = datetime.now().isoformat()
        unix_timestamp = int(time.time())
        log_entry = {
            "timestamp": timestamp,
            "unix_timestamp": unix_timestamp,
            "type": log_type,
            "message": message,
            "context": context or {}
        }
        
        # Write to file in a non-blocking way
        # Use a thread to avoid blocking the event loop
        threading.Thread(
            target=self._write_to_file,
            args=(log_entry,),
            daemon=True
        ).start()
        
        return True
    
    def log_sync(self, log_type: str, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Synchronous version of log method.
        
        Args:
            log_type: Type of log (error, access, or cron)
            message: The log message
            context: Optional dictionary of additional context
            
        Returns:
            bool: True if logging was successful
        """
        # Print to console
        print(f"[{log_type}] {message}")
        
        # Create log entry
        timestamp = datetime.now().isoformat()
        unix_timestamp = int(time.time())
        log_entry = {
            "timestamp": timestamp,
            "unix_timestamp": unix_timestamp,
            "type": log_type,
            "message": message,
            "context": context or {}
        }
        
        # Write synchronously
        self._write_to_file(log_entry)
        return True


