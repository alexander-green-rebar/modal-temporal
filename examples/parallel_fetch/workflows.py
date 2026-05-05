import asyncio
from datetime import timedelta
from typing import TypedDict

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import fetch_bytes, hash_bytes


class Fingerprint(TypedDict):
    size_bytes: int
    sha256: str


@workflow.defn
class FingerprintUrls:
    @workflow.run
    async def run(self, urls: list[str]) -> dict[str, Fingerprint]:
        # Phase 1: fetch all URLs concurrently.
        bytes_list = await asyncio.gather(
            *(
                workflow.execute_activity(
                    fetch_bytes,
                    url,
                    schedule_to_close_timeout=timedelta(seconds=120),
                )
                for url in urls
            )
        )
        # Phase 2: hash each blob concurrently.
        hashes = await asyncio.gather(
            *(
                workflow.execute_activity(
                    hash_bytes,
                    data,
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
                for data in bytes_list
            )
        )
        return {
            url: {"size_bytes": len(data), "sha256": h}
            for url, data, h in zip(urls, bytes_list, hashes)
        }
