import asyncio
import requests
import json
import time
from typing import List, Optional
from dotenv import load_dotenv
import os
from utils.logger import LoggerClient
from functools import partial
import aiohttp

logger = LoggerClient(token=os.getenv('LOGGER_TOKEN'))

load_dotenv()

class CloudflareIPUpdater:
    def __init__(self):
        """
        Initialize the CloudflareIPUpdater.
        """
        self.api_token = os.getenv("CLOUDFLARE_API_TOKEN")
        self.zone_id = os.getenv("CLOUDFLARE_ZONE_ID")
        self.record_names = os.getenv("CLOUDFLARE_RECORD_NAMES").split(",")
        self.current_ip: Optional[str] = None
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        self.base_url = "https://api.cloudflare.com/client/v4"

    async def get_current_ip(self) -> str:
        """Get the current public IP address."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.ipify.org?format=json") as response:
                    data = await response.json()
                    return data["ip"]
        except Exception as e:
            raise Exception(f"Failed to get current IP: {str(e)}")

    async def get_dns_record(self, record_name: str) -> Optional[dict]:
        """Get the DNS record details from Cloudflare."""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/zones/{self.zone_id}/dns_records"
                params = {"name": record_name, "type": "A"}
                async with session.get(url, headers=self.headers, params=params) as response:
                    data = await response.json()
                    records = data["result"]
                    return records[0] if records else None
        except Exception as e:
            raise Exception(f"Failed to get DNS record: {str(e)}")

    async def update_dns_record(self, record_id: str, record_name: str, new_ip: str) -> bool:
        """Update the DNS record with a new IP address."""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/zones/{self.zone_id}/dns_records/{record_id}"
                data = {
                    "type": "A",
                    "name": record_name,
                    "content": new_ip,
                    "proxied": True
                }
                
                async with session.put(url, headers=self.headers, json=data) as response:
                    if response.status == 200:
                        await logger.log("access", f"Updated DNS record for {record_name} to {new_ip}", "CloudflareIPUpdater")
                        return True
                    return False
        except Exception as e:
            await logger.log("error", f"Failed to update DNS record: {str(e)}", "CloudflareIPUpdater")
            return False

    async def check_and_update(self) -> None:
        """Check current IP and update DNS records if needed."""
        try:
            new_ip = await self.get_current_ip()
            
            if new_ip == self.current_ip:
                #await logger.log("access", f"IP hasn't changed; skipping.", "CloudflareIPUpdater")
                return
                
            await logger.log("access", f"IP changed: {self.current_ip} -> {new_ip}. DNS records updated.", "CloudflareIPUpdater")
            self.current_ip = new_ip
            
            for record_name in self.record_names:
                record = await self.get_dns_record(record_name)
                if not record:
                    await logger.log("error", f"No DNS record found for {record_name}", "CloudflareIPUpdater")
                    continue
                    
                if record["content"] != new_ip:
                    success = await self.update_dns_record(
                        record["id"], 
                        record_name, 
                        new_ip
                    )
                    if not success:
                        await logger.log("error", f"Failed to update {record_name}", "CloudflareIPUpdater")
                else:
                    await logger.log("access", f"Record {record_name} already has correct IP", "CloudflareIPUpdater")
                    
        except Exception as e:
            await logger.log("error", f"Error during check and update: {str(e)}", "CloudflareIPUpdater")

    async def start_monitoring(self, interval_seconds: int = 300) -> None:
        """Start monitoring IP changes at specified interval."""
        await logger.log("access", "Starting IP monitoring...", "CloudflareIPUpdater")
        while True:
            await self.check_and_update()
            await asyncio.sleep(interval_seconds)