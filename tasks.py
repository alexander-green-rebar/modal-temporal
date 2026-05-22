from textwrap import dedent
import os
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from invoke.tasks import task
import modal

app_name = "temporal-testing"
sandbox_name = "temporal-server"
project_root = Path(__file__).parent
temporal_home = (project_root / ".modal_temporal").absolute()
os.chdir(project_root)


@dataclass
class TemporalEndpoints:
    server: str
    ui: str
    metrics: str


def create_or_get_temporal_sandbox() -> TemporalEndpoints:
    app = modal.App.lookup(app_name, create_if_missing=True)
    image = modal.Image.from_registry("temporalio/temporal:1.7.0")

    try:
        sb = modal.Sandbox.create(
            "server",
            "start-dev",
            "--ip",
            "0.0.0.0",
            encrypted_ports=[8233, 53074],
            unencrypted_ports=[7233],
            image=image,
            name=sandbox_name,
            app=app,
            timeout=120 * 60,
        )
    except modal.exception.AlreadyExistsError:
        sb = modal.Sandbox.from_name(app_name=app_name, name=sandbox_name)

    tunnels = sb.tunnels()

    return TemporalEndpoints(
        server=f"{tunnels[7233].unencrypted_host}:{tunnels[7233].unencrypted_port}",
        ui=f"https://{tunnels[8233].host}",
        metrics=f"https://{tunnels[53074].host}/metrics",
    )


@task
def develop(ctx):
    endpoints = create_or_get_temporal_sandbox()

    source_content = dedent(f"""\
    export TEMPORAL_SERVER='{endpoints.server}'
    export TEMPORAL_UI='{endpoints.ui}'
    export TEMPORAL_METRICS='{endpoints.metrics}'
    export TEMPORAL_NAMESPACE='default'

    deactivate_modal_temporal () {{
        unset TEMPORAL_SERVER
        unset TEMPORAL_UI
        unset TEMPORAL_METRICS
        unset TEMPORAL_NAMESPACE
    }}""")
    temporal_home.mkdir(exist_ok=True)
    source_path = temporal_home / "activate"
    source_path.write_text(source_content)

    def make_link(url: str, label: str | None = None) -> str:
        label = label or url
        # OSC 8 hyperlink (supported by many modern terminals; harmless fallback elsewhere)
        return f"\x1b]8;;{url}\x1b\\{label}\x1b]8;;\x1b\\"

    ui_link = make_link(endpoints.ui, endpoints.ui)

    msg = dedent(f"""\
    🚀 Temporal started on Modal!
    🌎 Temporal UI: {ui_link}
    💻 Configure your local environment by running:

    source {source_path}""")
    print(msg)


@task
def stop(ctx):
    with suppress(Exception):
        sb = modal.Sandbox.from_name(app_name=app_name, name=sandbox_name)
        sb.terminate()
        ctx.run("uv run modal app stop temporal-testing --yes")

    print("sandbox terminated and App stopped!")
