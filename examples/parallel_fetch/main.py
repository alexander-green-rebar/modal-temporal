"""Parallel fetch + hash URLs.

Workflow ``FingerprintUrls`` takes a list of URLs and returns a per-URL
``{size_bytes, sha256}`` map. Two phases: fetch all URLs in parallel, then
hash each blob in parallel.

Run: uv run examples/parallel_fetch/main.py
"""

import modal
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import Fingerprint, FingerprintUrls
    from activities import fetch_bytes, hash_bytes

import modaltemporal as mt


# Demo only. In production, point at your own Temporal by setting
# TEMPORAL_SERVER and TEMPORAL_NAMESPACE, or passing them to mt.Worker.
server, namespace = mt.start_dev_temporal()

app = modal.App("modaltemporal-parallel-fetch")

image = (
    modal.Image.debian_slim()
    .uv_pip_install("temporalio==1.27.0")
    .add_local_python_source("activities", "workflows")
)

worker = mt.Worker(
    app,
    task_queue="parallel-fetch-queue",
    server=server,
    namespace=namespace,
    dispatcher_image=image,
)

worker.activity(image=image, max_inputs=50, timeout=180)(fetch_bytes)
worker.activity(image=image, max_inputs=4)(hash_bytes)
worker.workflow(FingerprintUrls)


async def main(client: mt.BoundClient) -> None:
    urls = [f"https://picsum.photos/seed/{i}/300/200" for i in range(15)]
    print(f"[parallel-fetch] starting workflow over {len(urls)} URLs…")

    handle = await client.start_workflow(
        FingerprintUrls,
        urls,
        id="fingerprint-demo",
    )
    results: dict[str, Fingerprint] = await handle.result()

    total_bytes = sum(r["size_bytes"] for r in results.values())
    print(
        f"[parallel-fetch] done: {len(results)} URLs, {total_bytes / 1024:.1f} KB total"
    )
    for url, info in list(results.items())[:5]:
        print(f"  {info['size_bytes']:>6} B  sha256={info['sha256'][:12]}…  {url}")


if __name__ == "__main__":
    worker.run(main)
