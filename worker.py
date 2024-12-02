import asyncio
import aiohttp
from datetime import datetime
from typing import Dict
import re
import json

class Worker:
    def __init__(self, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        metrics = {"error_rate": 0, "avg_response_time": 0, "request_count": 0}
        with open(filepath, "r") as file:
            file.seek(start)
            lines = file.read(size).splitlines()

        response_times = []
        error_count = 0
        request_count = 0
        for line in lines:
            log_entry = self.parse_log_line(line)
            if log_entry:
                if log_entry.level == "ERROR":
                    error_count += 1
                if "Request processed" in log_entry.message:
                    request_count += 1
                    match = re.search(r"(\d+)ms", log_entry.message)
                    if match:
                        response_times.append(int(match.group(1)))

        if request_count > 0:
            metrics["avg_response_time"] = sum(response_times) / len(response_times)
        if request_count > 0:
            metrics["error_rate"] = error_count / request_count
        metrics["request_count"] = request_count
        await self.report_results(metrics)
        return metrics

    def parse_log_line(self, line: str):
        try:
            timestamp_str, level, *message_parts = line.split(" ", 2)
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
            message = message_parts[0] if message_parts else ""
            return {"timestamp": timestamp, "level": level, "message": message}
        except ValueError:
            return None

    async def report_results(self, metrics: Dict) -> None:
        async with aiohttp.ClientSession() as session:
            await session.post(f"{self.coordinator_url}/metrics/{self.worker_id}", json=metrics)

    async def report_health(self) -> None:
        async with aiohttp.ClientSession() as session:
            await session.post(f"{self.coordinator_url}/heartbeat/{self.worker_id}")

