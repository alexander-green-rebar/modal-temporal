# Temporal Sandbox Snippet

This folder contains starter code to creating a Modal Sandbox that connects to a Temporal Client.

1. Create a [Modal Secret](https://modal.com/secrets) called `temporal-secret` with the env variable `TEMPORAL_API_KEY`.
2. Create a [Modal Volume](https://modal.com/docs/guide/volumes): `modal volume create sandbox-volume`
3. Upload the sample Temporal Workflow to the Volume: `modal volume put sandbox-volume hello_activity.py`. We've modified the existing [`hello_activity`](samples-python/hello/hello_activity.py) example, only modifying the client connection string.
4. Run the main script: `python main.py`

You can now navigate to your `sandbox-app`, click the Sandbox tab, and see the resulting logs!
