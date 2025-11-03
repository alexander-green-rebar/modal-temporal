import modal

TEMPORAL_PYTHON_REPO_URL = "https://github.com/temporalio/samples-python.git"

app = modal.App.lookup(name="sandbox-app", create_if_missing=True)
volume = modal.Volume.from_name("sandbox-volume")
image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("git")
    .run_commands(
        f"git clone {TEMPORAL_PYTHON_REPO_URL} && cd samples-python && uv sync"
    )
)

# Copies over the modified hello_activity.py from the Volume and runs it.
cmd = (
    "cp data/hello_activity.py /samples-python/hello/hello_activity.py && "
    + "cd /samples-python && "
    + "uv run hello/hello_activity.py"
)
sb = modal.Sandbox.create(
    "sh",
    "-c",
    cmd,
    app=app,
    image=image,
    volumes={"/data": volume},
    secrets=[modal.Secret.from_name("temporal-secret")],
)
