# modaltemporal

A single-file recipe for running Temporal activities as Modal Function invocations. Copy [`modaltemporal.py`](modaltemporal.py) into a project and adapt as needed.

The pattern: each `@worker.activity(...)` registration becomes its own Modal Function with independently configurable image, GPU, and concurrency. A single dispatcher Function polls the Temporal task queue and routes each activity task to the matching Modal Function via `Function.spawn` and Temporal's async-completion API.

## Run an example

```bash
uv sync
uv run examples/greeting/main.py
```

Examples bring up a dev Temporal server in a Modal Sandbox via `mt.start_dev_temporal()`. **In production, point at your own Temporal server** (Temporal Cloud, self-hosted). The dev sandbox is single-node and in-memory; useful for the examples here, not a deployment target.

To tear down the dev sandbox between runs:

```bash
uv run inv stop
```

## Examples

- [`examples/greeting/`](examples/greeting/): N workflows, each running 1 activity.
- [`examples/parallel_fetch/`](examples/parallel_fetch/): 1 workflow with two-phase activity fan-out. Demonstrates per-activity Modal config (I/O-bound vs CPU-bound concurrency).
- [`examples/per_item_pipeline/`](examples/per_item_pipeline/): N workflows, each running a sequential pipeline. Some URLs intentionally fail to demonstrate failure isolation and per-item retry policy.

## Shape

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


# Demo only. Replace with your own Temporal endpoint in production.
server, namespace = mt.start_dev_temporal()

app = modal.App("my-app")
image = modal.Image.debian_slim().uv_pip_install("temporalio==1.27.0")

worker = mt.Worker(
    app,
    task_queue="my-queue",
    server=server,
    namespace=namespace,
    dispatcher_image=image,
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
- `worker.deploy()`: persistent. Deploys the App and exits, while the dispatcher keeps polling the task queue (`min_containers=2` for redundancy, re-spawned every 30 min via Modal's schedule). For production where workflows arrive over time.

## Where things actually run

Activities and workflows look symmetric in registration (`worker.activity(...)`, `worker.workflow(...)`), but they don't run in the same place:

- **Activities** each get their own Modal Function. They scale independently, fanning out across as many containers as `max_inputs` and the spawn rate require.
- **Workflows** run *inside the dispatcher Function*. Workflow code is meant to be deterministic orchestration (it gets re-played on replay), so it's small and CPU-light by design. No separate Modal Function for it. If you ever have CPU-heavy workflow code or many concurrent workflows polling, the single-container dispatcher will be your bottleneck.

## Workflow / activity vocabulary

The `@activity.defn`, `@workflow.defn`, `workflow.execute_activity`, and `client.start_workflow` calls are unmodified Temporal SDK.

Everything inside `workflows.py` and `activities.py` is portable Temporal code.

The `mt.Worker` class replaces `temporalio.worker.Worker` to plumb each activity through Modal.