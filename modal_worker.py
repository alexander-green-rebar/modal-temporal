import re
import os
from datetime import timedelta
from typing import Any
from concurrent.futures import ThreadPoolExecutor

import modal
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

from modal_temporal import (
    run_activity,
    DispatchInterceptor,
)

APP_NAME = "temporal-testing"
app = modal.App(APP_NAME)

image = (
    modal.Image.debian_slim()
    .uv_pip_install("temporalio==1.27.0")
    .add_local_python_source("modal_temporal")
)


env: dict[str, str] = {
    "TEMPORAL_SERVER": os.environ["TEMPORAL_SERVER"],
    "TEMPORAL_NAMESPACE": os.environ["TEMPORAL_NAMESPACE"],
}


async def get_temporal_client() -> Client:
    return await Client.connect(
        env["TEMPORAL_SERVER"], namespace=env["TEMPORAL_NAMESPACE"]
    )


@activity.defn
async def greet(name: str) -> str:
    return f"Hello {name}"


@app.function(env=env, image=image, cpu=0.5)
async def greet_runner(args: Any, task_token: bytes) -> None:
    """Runs the `greet` activity.

    Note that, the dispatcher assumes that the modal function is named `{activity_name}_runner`."""
    client = await get_temporal_client()
    await run_activity(greet, args, client, task_token)


@activity.defn
def word_count(text: str) -> int:
    return len(re.findall(r"\b[a-zA-Z]+\b", text))


@app.function(env=env, image=image, cpu=1)
async def word_count_runner(args: Any, task_token: bytes) -> None:
    """Runs the `word_count` activity.
    Note that the dispatcher assumes that the modal function is named `{activity_name}_runner`."""
    client = await get_temporal_client()
    await run_activity(word_count, args, client, task_token)


@workflow.defn
class SayHelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> int:
        result = await workflow.execute_activity(
            greet, name, schedule_to_close_timeout=timedelta(seconds=30)
        )
        return await workflow.execute_activity(
            word_count, result, schedule_to_close_timeout=timedelta(seconds=30)
        )


@app.function(schedule=modal.Period(minutes=30), timeout=30 * 60, image=image, env=env)
async def queuer():
    """Pulls task from Temporal's task queue and immediately places it on Modal input queue.

    This function does not actually run the Temporal activity and should not take many resourc.es

    An alternative is to run this queuer on a machine external to Modal.
    """
    client = await get_temporal_client()
    worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[SayHelloWorkflow],
        activities=[greet, word_count],
        interceptors=[DispatchInterceptor(APP_NAME)],
        # Add a thread pool executor so we can run sync activities
        activity_executor=ThreadPoolExecutor(max_workers=1),
    )
    print("Dispatcher worker started. Activities will be completed by a Modal function")
    await worker.run()


if __name__ == "__main__":
    # Deploy and trigger the initial queuer
    with modal.enable_output():
        app.deploy()
        func = modal.Function.from_name("temporal-testing", "queuer")
        func.spawn()
