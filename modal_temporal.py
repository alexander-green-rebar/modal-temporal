import asyncio
import inspect
import modal
from typing import Coroutine, Any, Callable
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.worker import (
    Interceptor,
    ActivityInboundInterceptor,
    ExecuteActivityInput,
)
from temporalio.client import Client

HEARTBEAT_INTERVAL_SECONDS = 2.0


async def heartbeat_loop(
    handle, activity_name: str, activity_task: asyncio.Task
) -> None:
    """Temporal keeps a heartbeat to make sure the activity is running."""
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            await handle.heartbeat()
            print(f"[external worker] heartbeat sent for {activity_name}")
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"[external worker] heartbeat failed: {e}")
            # Heartbeat failure usually means Temporal cancelled or expired the activity.
            activity_task.cancel()
            return


async def run_activity(
    fn: Callable, args: Any, client: Client, task_token: bytes
) -> None:
    if inspect.iscoroutinefunction(fn):
        coro = fn(*args)
    else:
        coro = asyncio.to_thread(fn, *args)
    await run_activity_with_temporal(coro, fn.__name__, client, task_token)


async def run_activity_with_temporal(
    coro: Coroutine,
    activity_name: str,
    client: Client,
    task_token: bytes,
):
    handle = client.get_async_activity_handle(task_token=task_token)
    activity_task = asyncio.create_task(coro)
    hb_task = asyncio.create_task(heartbeat_loop(handle, activity_name, activity_task))

    try:
        result = await activity_task
        await handle.complete(result)
        print(f"[external worker] completed {activity_name} -> {result!r}")
    except asyncio.CancelledError:
        await handle.report_cancellation()
        return
    except Exception as e:
        await handle.fail(ApplicationError(str(e)))
        print(f"[external worker] failed {activity_name}: {e}")
    finally:
        hb_task.cancel()


class DispatchActivityInterceptor(ActivityInboundInterceptor):
    def __init__(
        self, next: ActivityInboundInterceptor, app_name: str, modal_func_cache: dict
    ) -> None:
        super().__init__(next)
        self._app_name = app_name
        self._modal_func_cache = modal_func_cache

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        info = activity.info()
        task_token = info.task_token
        activity_name = input.fn.__name__
        args = list(input.args)

        # Assumes that all functions are named `{activity_name}_runner`
        key = f"{activity_name}_runner"
        try:
            modal_func = self._modal_func_cache[key]
        except KeyError:
            modal_func = await modal.Function.from_name(
                self._app_name, key
            ).hydrate.aio()
            self._modal_func_cache[key] = modal_func

        print(f"[dispatcher] activity={activity_name} args={args} -> external worker")
        await modal_func.spawn.aio(args, task_token)
        activity.raise_complete_async()


class DispatchInterceptor(Interceptor):
    def __init__(self, app_name: str) -> None:
        self._app_name = app_name
        self._modal_func_cache = {}

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return DispatchActivityInterceptor(next, self._app_name, self._modal_func_cache)
