"""Per-item pipeline: one workflow per URL, each running fetch → word_count →
excerpt.

Some URLs intentionally fail (bad DNS, HTTP 500). Being per-item, this
isolates failures, applies per-workflow retry policy, and gives each URL its
own row in the Temporal UI.

Run: uv run examples/per_item_pipeline/main.py
"""

import asyncio

import modal
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import AnalyzeUrl, Summary
    from activities import excerpt, fetch_text, word_count

import modaltemporal as mt


app = modal.App("modaltemporal-per-item-pipeline")

image = (
    modal.Image.debian_slim()
    .uv_pip_install("temporalio==1.27.0")
    .add_local_python_source("activities", "workflows")
)

worker = mt.Worker(
    app,
    task_queue="per-item-pipeline-queue",
    dispatcher_image=image,
    auto_start_temporal=True,
)

# fetch_text — I/O-bound, tolerant of high concurrency per container.
worker.activity(image=image, max_inputs=20, timeout=60)(fetch_text)
# word_count / excerpt — CPU-bound, GIL-limited per container.
worker.activity(image=image, max_inputs=4)(word_count)
worker.activity(image=image, max_inputs=4)(excerpt)
worker.workflow(AnalyzeUrl)


# Mix of working URLs and ones that will fail.
URLS = [
    "https://www.python.org/about/",  # ok
    "https://example.com",  # ok
    "https://docs.python.org/3/",  # ok
    "https://nope-this-host-does-not-exist-zzz.invalid",  # fails: DNS
    "https://httpbin.org/status/500",  # fails: HTTP 500
]


async def main(client: mt.BoundClient) -> None:
    print(f"[per-item-pipeline] launching {len(URLS)} per-URL workflows…")
    handles = await asyncio.gather(
        *(
            client.start_workflow(
                AnalyzeUrl,
                url,
                id=f"analyze-{i}",
            )
            for i, url in enumerate(URLS)
        )
    )

    results = await asyncio.gather(
        *(h.result() for h in handles),
        return_exceptions=True,
    )

    succeeded: list[Summary] = []
    failed: list[tuple[str, BaseException]] = []
    for url, result in zip(URLS, results):
        if isinstance(result, BaseException):
            failed.append((url, result))
        else:
            succeeded.append(result)

    print(f"[per-item-pipeline] {len(succeeded)} succeeded, {len(failed)} failed")
    for s in succeeded:
        print(f"  [ok]   {s['words']:>5} words  {s['url']}")
        print(f"           {s['excerpt']}")
    for url, exc in failed:
        print(f"  [FAIL] {url}")
        print(f"           {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    worker.run(main)
