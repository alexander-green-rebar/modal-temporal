import asyncio
import uuid
from temporalio.client import Client
import os

TEMPORAL_SERVER = os.environ["TEMPORAL_SERVER"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]


async def main():
    client = await Client.connect(TEMPORAL_SERVER, namespace=TEMPORAL_NAMESPACE)
    workflows = [
        client.start_workflow(
            "SayHelloWorkflow",
            uuid.uuid4().hex[:8],
            id=f"say-hello-workflow-{uuid.uuid4()}",
            task_queue="my-task-queue",
        )
        for _ in range(500)
    ]
    await asyncio.gather(*workflows)


if __name__ == "__main__":
    asyncio.run(main())
