import modal

from foundry.common import APP_NAME

if __name__ == "__main__":
    # Build image manually.
    image_builder = modal.Function.from_name(APP_NAME, "image_builder")
    image_id = image_builder.remote()

    # Use image to run a worker and execute a workflow. 
    # The workflow will run in a Modal worker.
    worker_manager = modal.Cls.from_name(APP_NAME, "WorkerManager")()
    worker_manager.launch.remote(image_id)
    worker_manager.start_workflow.remote(image_id)
