"""Greeting: 20 independent workflows, each calling ``greet`` once.

Run: uv run examples/greeting/main.py
"""

import asyncio
import uuid

import modal
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import SayHelloWorkflow
    from activities import greet

import modaltemporal as mt


app = modal.App("modaltemporal-greeting")

image = (
    modal.Image.debian_slim()
    .uv_pip_install("temporalio==1.27.0")
    .add_local_python_source("activities", "workflows")
)

worker = mt.Worker(
    app,
    task_queue="greeting-queue",
    dispatcher_image=image,
    auto_start_temporal=True,
)

worker.activity(image=image, max_inputs=20)(greet)
worker.workflow(SayHelloWorkflow)


async def main(client: mt.BoundClient) -> None:
    handles = await asyncio.gather(
        *(
            client.start_workflow(
                SayHelloWorkflow,
                f"user-{i}",
                id=f"greet-{uuid.uuid4().hex[:8]}",
            )
            for i in range(20)
        )
    )
    results: list[str] = await asyncio.gather(*(h.result() for h in handles))
    print(f"[greeting] {len(results)} workflows completed")
    for r in results[:3]:
        print(f"  → {r}")


if __name__ == "__main__":
    worker.run(main)
