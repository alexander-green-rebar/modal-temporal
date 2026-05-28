import re
import os
from datetime import timedelta

import argparse
import asyncio
import modal
from temporalio import workflow, activity
import uuid

from modal_temporal import (
    modal_activity,
    modal_activity_cls,
    modal_activity_method,
    run_dispatcher,
    get_temporal_client,
)

APP_NAME = "temporal-testing"
app = modal.App(APP_NAME)
TASK_QUEUE_NAME = "my-task-queue"

image = (
    modal.Image.debian_slim()
    .uv_pip_install("temporalio==1.27.0", "async-lru==2.3.0")
    .add_local_python_source("modal_temporal")
)


env: dict[str, str | None] = {
    "TEMPORAL_SERVER_URL": os.environ["TEMPORAL_SERVER_URL"],
    "TEMPORAL_NAMESPACE": os.environ["TEMPORAL_NAMESPACE"],
}


@modal_activity(app, env=env, image=image)
async def get_work(amount: int) -> list[str]:
    import random

    words = [
        "the",
        "quick",
        "brown",
        "fox",
        "jumps",
        "over",
        "lazy",
        "dog",
        "hello",
        "world",
    ]
    return [
        " ".join(random.choices(words, k=random.randint(3, 10))) for _ in range(amount)
    ]


@modal_activity(app, env=env, image=image)
@modal.concurrent(max_inputs=2)
def word_count(text: str) -> int:
    return len(re.findall(r"\b[a-zA-Z]+\b", text))


# Class based activity
@modal_activity_cls(app, env=env, image=image)
class AddValue:
    value: int = modal.parameter()

    @modal_activity_method
    async def run(self, input_value: int) -> int:
        return self.value + input_value


@modal_activity(app, env=env, image=image)
def reduce_values(value: list[int]) -> int:
    activity.logger.info("Reducing values")
    return sum(value)


@workflow.defn
class SayHelloWorkflow:
    @workflow.run
    async def run(self, amount: int) -> int:
        work_items = await workflow.execute_activity(
            get_work, amount, schedule_to_close_timeout=timedelta(seconds=30)
        )

        async def process_single_item(item: str) -> int:
            count = await workflow.execute_activity(
                word_count, item, schedule_to_close_timeout=timedelta(seconds=30)
            )
            return await workflow.execute_activity_method(
                AddValue.run, count, schedule_to_close_timeout=timedelta(seconds=30)
            )

        count_tasks = [process_single_item(item) for item in work_items]
        counts = await asyncio.gather(*count_tasks)
        return await workflow.execute_activity(
            reduce_values, counts, schedule_to_close_timeout=timedelta(seconds=30)
        )


@app.cls(min_containers=1, image=image, env=env)
class Enqueuer:
    @modal.enter()
    async def start(self):
        """Pulls task from Temporal's task queue and immediately places it on Modal input queue.

        This function does not actually run the Temporal activity and should not take many resources.

        An alternative is to run this queuer on a machine external to Modal.
        """
        await run_dispatcher(
            APP_NAME,
            task_queue=TASK_QUEUE_NAME,
            workflows=[SayHelloWorkflow],
            activities=[get_work, word_count, reduce_values, AddValue(value=4).run],
        )


@app.function(image=image, env=env)
async def launch_workflows(count: int, amount: int):
    client = await get_temporal_client()
    workflows = [
        client.start_workflow(
            "SayHelloWorkflow",
            amount,
            id=f"say-hello-workflow-{uuid.uuid4()}",
            task_queue=TASK_QUEUE_NAME,
        )
        for _ in range(count)
    ]
    await asyncio.gather(*workflows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--count", type=int, default=1, help="Number of workflows to launch"
    )
    parser.add_argument(
        "--work", type=int, default=20, help="Number of workflows to launch"
    )
    args = parser.parse_args()
    func = modal.Function.from_name(APP_NAME, "launch_workflows")
    # Launch workflow
    req = func.spawn(args.count, args.work)
    print(f"Launched workflow at: {req.get_dashboard_url()}")
