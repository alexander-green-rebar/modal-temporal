from foundry.app import app, temporal_volume


# Sync config files manually. This is designed to be integrated with CI/CD.
@app.local_entrypoint()
def sync_files():
    with temporal_volume.batch_upload() as batch:
        batch.put_directory("./workflows", "/workflows")
        batch.put_directory("./activities", "/activities")
        batch.put_directory("./configs", "/configs")
