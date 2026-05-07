import argparse
import asyncio
import uuid
from temporalio.client import Client
import os

TEMPORAL_SERVER = os.environ["TEMPORAL_SERVER"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]


async def main(count: int):
    client = await Client.connect(TEMPORAL_SERVER, namespace=TEMPORAL_NAMESPACE)
    workflows = [
        client.start_workflow(
            "SayHelloWorkflow",
            "This is a name",
            id=f"say-hello-workflow-{uuid.uuid4()}",
            task_queue="my-task-queue",
        )
        for _ in range(count)
    ]
    await asyncio.gather(*workflows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--count", type=int, default=100, help="Number of workflows to launch"
    )
    args = parser.parse_args()
    asyncio.run(main(args.count))
