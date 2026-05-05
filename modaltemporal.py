"""modaltemporal: run Temporal activities as Modal function invocations.

A ``Worker`` deploys two kinds of Modal Functions to a single Modal App:

  - One **dispatcher** Function. It runs a Temporal Worker that polls the configured
    Task Queue (one queue per Worker, shared across all registered Activities). For
    each Task it receives, it simply spawns a matching Modal Activity Function and
    communicates to Temporal that the Task will be completed asynchronously, and not
    to block on it.

  - One **Activity** Function per ``@worker.activity(...)`` registration. Each is
    configured independently (image, GPU, max_inputs, timeout) and autoscales on
    its own. When invoked, it runs the Activity body, heartbeats Temporal while
    it works, and reports success / failure / cancellation back using the
    Activity's `task_token`.

The wiring relies on two primitives:

  - Modal's ``Function.spawn``: a fire-and-forget invocation. The dispatcher returns
    immediately with a handle instead of awaiting the Activity, so it never blocks
    on Activity work and can move straight to the next Task off the Queue.

  - Temporal's async-completion API (``raise_complete_async`` + ``task_token``):
    decouples Activity completion from the Worker that received the Task. The
    `task_token` is a handle Temporal mints per Activity attempt; any process
    holding it can call `.complete()` / `.fail()` / `.heartbeat()` on behalf of
    the Activity. This lets the spawned Modal Function report back to Temporal.
"""

import asyncio
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import modal
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
    Worker as _TemporalWorker,
)


_TEMPORAL_SANDBOX_APP = "temporal-testing"
_TEMPORAL_SANDBOX_NAME = "temporal-server"


def _activity_name(fn: Callable) -> str:
    """Return the Temporal-registered name for an activity function.

    Honors ``@activity.defn(name="custom")`` instead of just ``fn.__name__``.
    """
    defn = getattr(fn, "__temporal_activity_definition", None)
    if defn is not None:
        name = getattr(defn, "name", None)
        if name:
            return name
    return fn.__name__


def _with_modaltemporal(image: modal.Image) -> modal.Image:
    """Bake the ``modaltemporal`` module into a Modal Image.

    Auto-applied to every image the SDK uses (dispatcher + per-activity), so
    users don't need to remember ``add_local_python_source("modaltemporal")``.
    Once the package is published to PyPI, swap this for ``pip_install``.
    """
    return image.add_local_python_source("modaltemporal")


def _ensure_temporal_sandbox() -> tuple[str, str, str]:
    """Idempotently start a Temporal server in a Modal Sandbox.

    DEV ONLY: this runs Temporal's ``start-dev`` (single-node, in-memory
    persistence). Not for production. Returns ``(server, namespace, ui_url)``.
    Reuses an existing Sandbox of the same name if alive; replaces it otherwise.
    """
    print(
        "[modaltemporal] WARNING: starting a DEV-MODE Temporal server "
        "(start-dev, in-memory). Not for production — bring your own Temporal "
        "for any persistent workload."
    )
    sandbox_app = modal.App.lookup(_TEMPORAL_SANDBOX_APP, create_if_missing=True)
    image = modal.Image.from_registry("temporalio/temporal:1.7.0")

    def _create() -> modal.Sandbox:
        return modal.Sandbox.create(
            "server",
            "start-dev",
            "--ip",
            "0.0.0.0",
            encrypted_ports=[8233],
            unencrypted_ports=[7233],
            image=image,
            name=_TEMPORAL_SANDBOX_NAME,
            app=sandbox_app,
            timeout=60 * 60,
        )

    try:
        sb = _create()
    except modal.exception.AlreadyExistsError:
        sb = modal.Sandbox.from_name(
            app_name=_TEMPORAL_SANDBOX_APP, name=_TEMPORAL_SANDBOX_NAME
        )
        # If the existing Sandbox already terminated (timed out, crashed), its
        # tunnels point nowhere — replace it.
        if sb.poll() is not None:
            print("[modaltemporal] previous Temporal Sandbox is dead; recreating…")
            with suppress(Exception):
                sb.terminate()
            sb = _create()

    tunnels = sb.tunnels()
    server = f"{tunnels[7233].unencrypted_host}:{tunnels[7233].unencrypted_port}"
    ui = f"https://{tunnels[8233].host}"
    return server, "default", ui


@dataclass
class _ActivityReg:
    fn: Callable
    runner_name: str
    runner_fn: modal.Function


class BoundClient:
    """Wrapper around ``temporalio.client.Client`` that defaults the
    ``task_queue`` argument on ``start_workflow`` calls."""

    def __init__(self, client: Client, task_queue: str) -> None:
        self._client = client
        self._task_queue = task_queue

    async def start_workflow(self, *args, **kwargs):
        kwargs.setdefault("task_queue", self._task_queue)
        return await self._client.start_workflow(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class Worker:
    def __init__(
        self,
        app: modal.App,
        *,
        task_queue: str,
        server: str | None = None,
        namespace: str | None = None,
        dispatcher_image: modal.Image | None = None,
        auto_start_temporal: bool = False,
    ) -> None:
        ui_url: str | None = None
        if auto_start_temporal and server is None:
            # auto_start_temporal owns the Sandbox lifecycle — ignore TEMPORAL_SERVER
            # env vars in this path so a stale activate file doesn't silently
            # redirect us to a dead Sandbox.
            print("[modaltemporal] auto_start_temporal=True; ensuring Sandbox…")
            server, ns, ui_url = _ensure_temporal_sandbox()
            namespace = namespace or ns
            print(f"[modaltemporal] Temporal UI: {ui_url}")

        self._app = app
        self._task_queue = task_queue
        self._server = server or os.environ["TEMPORAL_SERVER"]
        self._namespace = namespace or os.environ.get("TEMPORAL_NAMESPACE", "default")
        self._temporal_ui = ui_url or os.environ.get("TEMPORAL_UI")
        self._dispatcher_image = _with_modaltemporal(
            dispatcher_image
            or modal.Image.debian_slim().uv_pip_install("temporalio==1.27.0")
        )
        self._activities: dict[str, _ActivityReg] = {}
        self._workflows: list[type] = []
        self._client: BoundClient | None = None
        self._client_lock = asyncio.Lock()
        self._dispatcher_fn: modal.Function | None = None

    async def client(self) -> "BoundClient":
        """Connect to Temporal and return a Client bound to this Worker's task queue.

        Cached: repeated calls reuse the same underlying ``temporalio.client.Client``.
        """
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    raw = await Client.connect(self._server, namespace=self._namespace)
                    self._client = BoundClient(raw, self._task_queue)
        return self._client

    async def start_workflow(self, *args, **kwargs):
        """Start a workflow on this Worker's task queue."""
        client = await self.client()
        return await client.start_workflow(*args, **kwargs)

    def activity(
        self,
        *,
        image: modal.Image,
        max_inputs: int = 20,
        gpu: str | None = None,
        memory: int | None = None,
        timeout: int | None = None,
        cpu: float | None = None,
    ) -> Callable[[Callable], Callable]:
        def decorator(fn: Callable) -> Callable:
            self._register_activity(
                fn,
                image=image,
                max_inputs=max_inputs,
                gpu=gpu,
                memory=memory,
                timeout=timeout,
                cpu=cpu,
            )
            return fn

        return decorator

    def workflow(self, cls: type) -> type:
        self._workflows.append(cls)
        return cls

    def _register_activity(
        self,
        fn: Callable,
        *,
        image: modal.Image,
        max_inputs: int,
        gpu: str | None,
        memory: int | None,
        timeout: int | None,
        cpu: float | None,
    ) -> None:
        # Use the Temporal-resolved name (honors ``@activity.defn(name="...")``)
        # as the registration key, but use fn.__name__ for the Modal Function
        # name (always a valid identifier).
        registered_name = _activity_name(fn)
        runner_name = f"_mt_run_{fn.__name__}"
        server, namespace = self._server, self._namespace

        async def _runner(
            task_token: bytes, args: list, heartbeat_seconds: float | None
        ) -> None:
            await _run_activity(
                fn, task_token, args, server, namespace, heartbeat_seconds
            )

        _runner.__name__ = runner_name
        _runner.__qualname__ = runner_name

        fn_kwargs: dict[str, Any] = {
            "image": _with_modaltemporal(image),
            "env": {"TEMPORAL_SERVER": server, "TEMPORAL_NAMESPACE": namespace},
            "name": runner_name,
            "serialized": True,
        }
        if gpu is not None:
            fn_kwargs["gpu"] = gpu
        if memory is not None:
            fn_kwargs["memory"] = memory
        if timeout is not None:
            fn_kwargs["timeout"] = timeout
        if cpu is not None:
            fn_kwargs["cpu"] = cpu

        runner_fn = self._app.function(**fn_kwargs)(
            modal.concurrent(max_inputs=max_inputs)(_runner)
        )

        self._activities[registered_name] = _ActivityReg(
            fn=fn, runner_name=runner_name, runner_fn=runner_fn
        )

    def _register_dispatcher(self, *, with_schedule: bool) -> None:
        server = self._server
        namespace = self._namespace
        task_queue = self._task_queue
        activity_regs = list(self._activities.values())
        workflows = list(self._workflows)

        async def _dispatcher() -> None:
            await _run_dispatcher(
                server, namespace, task_queue, activity_regs, workflows
            )

        _dispatcher.__name__ = "_mt_dispatcher"
        _dispatcher.__qualname__ = "_mt_dispatcher"

        fn_kwargs: dict[str, Any] = {
            "image": self._dispatcher_image,
            "timeout": 30 * 60,
            "env": {"TEMPORAL_SERVER": server, "TEMPORAL_NAMESPACE": namespace},
            "name": "_mt_dispatcher",
            "serialized": True,
        }
        if with_schedule:
            fn_kwargs["schedule"] = modal.Period(minutes=30)
            # Two dispatchers polling the same task queue. Temporal delivers
            # each task to exactly one worker. In case one container dies.
            fn_kwargs["min_containers"] = 2
        self._dispatcher_fn = self._app.function(**fn_kwargs)(_dispatcher)

    def deploy(self) -> None:
        """Persistent / production mode: deploy the App and start polling.

        The dispatcher and activity Functions stay alive on Modal after this script
        exits. Use this when workflows arrive asynchronously over time.
        """
        self._register_dispatcher(with_schedule=True)

        with modal.enable_output():
            self._app.deploy()
            modal.Function.from_name(self._app.name, "_mt_dispatcher").spawn()

        app_id = self._app.app_id
        app_name = self._app.name
        print()
        print(
            f"[modaltemporal] deployed: {app_name}" + (f" ({app_id})" if app_id else "")
        )
        if app_id:
            print(f"  Tail logs:    modal app logs {app_id}")
            print(f"  Dashboard:    https://modal.com/id/{app_id}")
        else:
            print(f"  Tail logs:    modal app logs {app_name}")
        print(f"  Stop:         modal app stop {app_name}")

    def run(self, main: Callable[["BoundClient"], Awaitable[Any]]) -> Any:
        """Ephemeral: spin up the App, run ``main(client)``, tear down.

        Brings the App up via ``modal.App.run()``, spawns the dispatcher, calls
        ``main`` with a task-queue-bound Client, and waits for it to return.
        When ``main`` returns, the App is torn down. So, ``main`` should
        ``await`` every workflow result before returning.
        """
        self._register_dispatcher(with_schedule=False)
        dispatcher_fn = self._dispatcher_fn
        assert dispatcher_fn is not None

        async def _orchestrate() -> Any:
            await dispatcher_fn.spawn.aio()
            client = await self.client()
            if self._temporal_ui:
                print(f"[modaltemporal] Temporal UI: {self._temporal_ui}")
            return await main(client)

        with modal.enable_output():
            with self._app.run():
                return asyncio.run(_orchestrate())


# -----------------------------------------------------------------------------
# Module-level entrypoints. The cache below is per-Modal-container. Within one
# container, all invocations reuse a single Temporal Client + gRPC channel.
# -----------------------------------------------------------------------------


_CLIENT_CACHE: dict[tuple[str, str], Client] = {}
_CLIENT_CACHE_LOCK: asyncio.Lock | None = None


async def _get_client(server: str, namespace: str) -> Client:
    global _CLIENT_CACHE_LOCK
    if _CLIENT_CACHE_LOCK is None:
        _CLIENT_CACHE_LOCK = asyncio.Lock()
    key = (server, namespace)
    if key not in _CLIENT_CACHE:
        async with _CLIENT_CACHE_LOCK:
            if key not in _CLIENT_CACHE:
                _CLIENT_CACHE[key] = await Client.connect(server, namespace=namespace)
    return _CLIENT_CACHE[key]


async def _run_dispatcher(
    server: str,
    namespace: str,
    task_queue: str,
    activity_regs: list[_ActivityReg],
    workflows: list[type],
) -> None:
    client = await _get_client(server, namespace)

    # Key by Temporal-registered name — matches what the interceptor sees via
    # ``activity.info().activity_type`` and honors ``@activity.defn(name=...)``.
    funcs: dict[str, modal.Function] = {
        _activity_name(reg.fn): reg.runner_fn for reg in activity_regs
    }

    print(f"[modaltemporal] dispatcher started: task_queue={task_queue!r}")
    temporal_worker = _TemporalWorker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=[reg.fn for reg in activity_regs],
        interceptors=[_DispatchFactory(funcs)],
    )
    try:
        await temporal_worker.run()
    except asyncio.CancelledError:
        # Cancellation is the dispatcher's natural shutdown signal (ephemeral
        # mode tears it down when main returns; manual cancel does the same).
        # Exit cleanly so the call doesn't surface as an unhandled exception.
        print(f"[modaltemporal] dispatcher shutdown: task_queue={task_queue!r}")


async def _run_activity(
    fn: Callable,
    task_token: bytes,
    args: list,
    server: str,
    namespace: str,
    heartbeat_timeout_seconds: float | None,
) -> None:
    client = await _get_client(server, namespace)
    handle = client.get_async_activity_handle(task_token=task_token)

    main_task = asyncio.create_task(fn(*args))
    hb_task: asyncio.Task | None = None
    if heartbeat_timeout_seconds:
        # Beat at 1/3 of the timeout so a single dropped heartbeat won't kill the activity.
        interval = max(1.0, heartbeat_timeout_seconds / 3)
        hb_task = asyncio.create_task(_heartbeat_loop(handle, interval, main_task))

    try:
        result = await main_task
    except asyncio.CancelledError:
        await handle.report_cancelled()
        return
    except Exception as e:
        await handle.fail(e)
        return
    finally:
        if hb_task is not None:
            hb_task.cancel()

    await handle.complete(result)


async def _heartbeat_loop(handle, interval: float, main_task: asyncio.Task) -> None:
    while True:
        try:
            await asyncio.sleep(interval)
            await handle.heartbeat()
        except asyncio.CancelledError:
            return
        except Exception:
            # Heartbeat failure usually means Temporal cancelled or expired the activity.
            main_task.cancel()
            return


class _DispatchInbound(ActivityInboundInterceptor):
    def __init__(
        self, next: ActivityInboundInterceptor, funcs: dict[str, modal.Function]
    ) -> None:
        super().__init__(next)
        self._funcs = funcs

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        info = activity.info()
        # Use the Temporal-registered activity name (honors @activity.defn(name=...)).
        registered_name = info.activity_type
        modal_func = self._funcs.get(registered_name)
        if modal_func is None:
            raise RuntimeError(
                f"no Modal runner registered for activity {registered_name!r}"
            )

        heartbeat_seconds = (
            info.heartbeat_timeout.total_seconds() if info.heartbeat_timeout else None
        )

        await modal_func.spawn.aio(info.task_token, list(input.args), heartbeat_seconds)
        activity.raise_complete_async()


class _DispatchFactory(Interceptor):
    def __init__(self, funcs: dict[str, modal.Function]) -> None:
        self._funcs = funcs

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _DispatchInbound(next, self._funcs)
