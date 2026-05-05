# modaltemporal

Run Temporal activities as Modal Function invocations. Each `@worker.activity(...)` registration becomes its own Modal Function with independently configurable image, GPU, and concurrency. A single dispatcher Function polls the Temporal task queue and routes each activity task to the matching runner via `Function.spawn` and Temporal's async-completion API.

## Run

```bash
uv sync
uv run examples/greeting/main.py
```

The first run auto-starts a Temporal server in a Modal Sandbox if `TEMPORAL_SERVER` isn't set. To tear that Sandbox down:

```bash
uv run inv stop
```

## Examples

- [`examples/greeting/`](examples/greeting/): N workflows, each runing 1 activity.
- [`examples/parallel_fetch/`](examples/parallel_fetch/): 1 workflow with two-stage activity.
- [`examples/per_item_pipeline/`](examples/per_item_pipeline/): N workflows, each running a sequential pipeline. Some URLs intentionally fail to demonstrate the failure isolation and per-item retry policy.

## SDK shape

```python
import modal
import modaltemporal as mt
from datetime import timedelta
from temporalio import activity, workflow


@activity.defn
async def greet(name: str) -> str:
    return f"Hello {name}"


@workflow.defn
class SayHello:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            greet, name, schedule_to_close_timeout=timedelta(seconds=10)
        )


app = modal.App("my-app")
image = modal.Image.debian_slim().uv_pip_install("temporalio==1.27.0")

worker = mt.Worker(
    app,
    task_queue="my-queue",
    dispatcher_image=image,
    auto_start_temporal=True,
)
worker.activity(image=image, max_inputs=20)(greet)
worker.workflow(SayHello)


async def main(client: mt.BoundClient) -> None:
    handle = await client.start_workflow(SayHello, "world", id="hello-1")
    print(await handle.result())


if __name__ == "__main__":
    worker.run(main)         # ephemeral: bring App up, run main, tear down
    # worker.deploy()        # persistent: deploy and exit; dispatcher keeps polling
```

## Lifecycle modes

- `worker.run(main)`: ephemeral. Spins the App up, runs your async `main(client)`, tears down on return. For demos and scripts that drive their own workflows.
- `worker.deploy()`: persistent. Deploys the App and exits,while the dispatcher keeps polling the task queue (re-spawned every 30 min via Modal's schedule). For production where workflows arrive over time.

## Workflow / activity vocabulary

The `@activity.defn`, `@workflow.defn`, `workflow.execute_activity`, and `client.start_workflow` calls are unmodified Temporal SDK. 

The `mt.Worker` class replaces `temporalio.worker.Worker` to pass each activity through Modal. 

Everything inside `workflows.py` and `activities.py` is portable Temporal code.
