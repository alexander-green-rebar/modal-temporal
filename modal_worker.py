import asyncio
from typing import Any
import os
import modal
from temporalio.client import Client
from activities import greet

from temporalio.exceptions import ApplicationError
import os
import modal
from typing import Any
from temporalio.client import Client
from temporalio.worker import Worker

from temporalio.worker import (
    Worker,
    Interceptor,
    ActivityInboundInterceptor,
    ExecuteActivityInput,
)
from temporalio import activity
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import SayHelloWorkflow
    from activities import greet

app = modal.App("temporal-testing")

image = (
    modal.Image.debian_slim()
    .uv_pip_install("temporalio==1.27.0")
    .add_local_python_source("activities", "workflows")
)


TEMPORAL_SERVER = os.environ["TEMPORAL_SERVER"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]
HEARTBEAT_INTERVAL_SECONDS = 2.0


ACTIVITIES = {"greet": greet}

env: dict[str, str] = {
    "TEMPORAL_SERVER": TEMPORAL_SERVER,
    "TEMPORAL_NAMESPACE": TEMPORAL_NAMESPACE,
}


async def heartbeat_loop(handle, activity_name: str) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        try:
            await handle.heartbeat()
            print(f"[external worker] heartbeat sent for {activity_name}")
        except Exception as e:
            print(f"[external worker] heartbeat failed: {e}")
            return


@app.function(env=env, image=image)
@modal.concurrent(max_inputs=20)
async def run_activity(task_token: bytes, activity_name: str, args: Any) -> None:
    client = await Client.connect(TEMPORAL_SERVER, namespace=TEMPORAL_NAMESPACE)
    handle = client.get_async_activity_handle(task_token=task_token)
    fn = ACTIVITIES.get(activity_name)

    hb_task = asyncio.create_task(heartbeat_loop(handle, activity_name))

    try:
        if fn is None:
            raise RuntimeError(f"unknown activity {activity_name}")
        result = await fn(*args)
        await handle.complete(result)
        print(f"[external worker] completed {activity_name} -> {result!r}")
    except Exception as e:
        await handle.fail(ApplicationError(str(e)))
        print(f"[external worker] failed {activity_name}: {e}")

    finally:
        hb_task.cancel()


class DispatchActivityInterceptor(ActivityInboundInterceptor):
    def __init__(
        self, next: ActivityInboundInterceptor, modal_func: modal.Function
    ) -> None:
        super().__init__(next)
        self._modal_func = modal_func

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        info = activity.info()
        task_token = info.task_token
        activity_name = input.fn.__name__
        args = list(input.args)

        print(f"[dispatcher] activity={activity_name} args={args} -> external worker")
        await run_activity.spawn.aio(task_token, activity_name, args)
        activity.raise_complete_async()


class DispatchInterceptor(Interceptor):
    def __init__(self, modal_func: modal.Function) -> None:
        self._modal_func = modal_func

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return DispatchActivityInterceptor(next, self._modal_func)


@app.function(schedule=modal.Period(minutes=30), timeout=30 * 60, image=image, env=env)
async def queuer():
    client = await Client.connect(TEMPORAL_SERVER, namespace=TEMPORAL_NAMESPACE)
    modal_func = await modal.Function.from_name(
        "temporal-testing", "run_activity"
    ).hydrate.aio()
    worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[SayHelloWorkflow],
        activities=[greet],
        interceptors=[DispatchInterceptor(modal_func)],
    )
    print("Dispatcher worker started. Activities will be completed by a Modal function")
    await worker.run()


if __name__ == "__main__":
    # Deploy and trigger the first queuer
    with modal.enable_output():
        app.deploy()
        func = modal.Function.from_name("temporal-testing", "queuer")
        func.spawn()
