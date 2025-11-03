import modal

from foundry.common import logger, APP_NAME


# Base image has conda, temporal, and possibly other base requirements.
base_image = (
    modal.Image.debian_slim()
    .apt_install("wget")
    .pip_install("temporalio==1.14.1", "pyyaml")
    .run_commands(
        "wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh",
        "bash /tmp/miniconda.sh -b -p /opt/conda",
        "rm /tmp/miniconda.sh",
        "/opt/conda/bin/conda clean -afy",
        # Accept ToS in different Conda channels
        *[
            f"/opt/conda/bin/conda tos accept --override-channels --channel {channel}" for channel in [
                "https://repo.anaconda.com/pkgs/main",
                "https://repo.anaconda.com/pkgs/r",
            ]
        ],
    )
    .add_local_python_source("foundry")
    .add_local_file("./worker.py", "/root/worker.py")
)
app = modal.App(APP_NAME, image=base_image)

# We will use a Volume to store worklow and activity definitions, as well
# as configs used by this worker.
temporal_volume = modal.Volume.from_name("temporal-workflows", create_if_missing=True)
volumes = {
    "/temporal": temporal_volume,
}
secrets = [
    modal.Secret.from_name(
        "temporal-secrets",
        required_keys=[
            "TEMPORAL_SERVER_URL",
            "TEMPORAL_API_KEY",
            "TEMPORAL_NAMESPACE",
        ],
    )
]

@app.function(
    schedule=modal.Period(minutes=60),
    volumes=volumes,
)
def image_builder() -> str | None:
    """Build Modal images from conda definitions."""
    sb = modal.Sandbox.create(app=app, image=base_image)

    p = sb.exec("/opt/conda/bin/conda", "install", "-y", "numpy")
    p.wait()
    logger.info(p.stdout.read())
    logger.info(p.stderr.read())


    image_id = None
    if p.returncode != 0:
        logger.error("failed to create conda environment")
    else:
        image = sb.snapshot_filesystem()
        image_id = image.object_id
        logger.info(f"created conda image: {image_id}")

    sb.terminate()
    return image_id


@app.cls()
class WorkerManager:
    
    @modal.method()
    def launch(self, image_id: str, pool:str = "default"):
        image = modal.Image.from_id(image_id)
        sb = modal.Sandbox.create(
            "python", "/root/worker.py", "--mode", "worker", "--config", f"{pool}.yml",
            app=app, image=image, volumes=volumes,
            secrets=secrets, timeout=60 * 5,
        )
        sb.set_tags({"image_id": image_id})
        print(f"worker {sb.object_id} created in pool {pool}")

    @modal.method()
    def start_workflow(self, image_id:str, workflow:str = "ExampleWorkflow"):
        image = modal.Image.from_id(image_id)
        sb = modal.Sandbox.create(
            "python", "/root/worker.py", "--mode", "workflow", "--workflow", workflow,
            app=app, image=image, volumes=volumes, secrets=secrets, timeout=60 * 5,
        )
        sb.set_tags({"image_id": image_id})
        print(f"{workflow=} running in {sb.object_id}")
        
