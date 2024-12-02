import asyncio
from aiohttp import web
import random
from typing import Dict

class Coordinator:
    def __init__(self, port: int):
        self.port = port
        self.workers = {}
        self.results = {}

    async def distribute_work(self, filepath: str) -> None:
        chunk_size = 1024
        total_size = 10000
        chunks = [(i * chunk_size, chunk_size) for i in range(total_size // chunk_size)]

        worker_ids = list(self.workers.keys())
        for i, chunk in enumerate(chunks):
            worker_id = worker_ids[i % len(worker_ids)]
            await self.assign_chunk(worker_id, filepath, chunk)

    async def assign_chunk(self, worker_id: str, filepath: str, chunk: tuple) -> None:
        start, size = chunk
        worker_url = self.workers[worker_id]
        async with aiohttp.ClientSession() as session:
            await session.post(f"{worker_url}/process", json={"filepath": filepath, "start": start, "size": size})

    async def handle_metrics(self, request: web.Request):
        worker_id = request.match_info["worker_id"]
        metrics = await request.json()
        self.results[worker_id] = metrics
        return web.Response(text="Metrics received")

    async def handle_heartbeat(self, request: web.Request):
        worker_id = request.match_info["worker_id"]
        return web.Response(text="Heartbeat received")

    async def run(self):
        app = web.Application()
        app.add_routes([web.post('/metrics/{worker_id}', self.handle_metrics)])
        app.add_routes([web.post('/heartbeat/{worker_id}', self.handle_heartbeat)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        print(f"Coordinator listening on port {self.port}")

    def register_worker(self, worker_id: str, worker_url: str):
        self.workers[worker_id] = worker_url

    def get_aggregated_metrics(self) -> Dict:
        aggregated = {"error_rate": 0, "avg_response_time": 0, "request_count": 0}
        total_workers = len(self.results)
        if total_workers == 0:
            return aggregated

        for worker_metrics in self.results.values():
            aggregated["error_rate"] += worker_metrics["error_rate"]
            aggregated["avg_response_time"] += worker_metrics["avg_response_time"]
            aggregated["request_count"] += worker_metrics["request_count"]

        aggregated["error_rate"] /= total_workers
        aggregated["avg_response_time"] /= total_workers
        return aggregated

async def main():
    coordinator = Coordinator(8000)
    await coordinator.run()

if __name__ == '__main__':
    asyncio.run(main())

