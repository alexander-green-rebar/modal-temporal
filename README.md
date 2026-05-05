# Modal Temporal Demo

## Instructions

0. Clone this branch

```bash
git clone -b thomasjpfan/native-worker https://github.com/modal-projects/modal-temporal
cd modal-temporal
```

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/)

2. Deploy temporal server in a Modal Sandbox. (Only used for testing)

```bash
uv run inv develop
```

which should output:

```
🚀 Temporal started on Modal!
🌎 Temporal UI: ...w.modal.host
💻 Configure your local environment by running:

source .modal_temporal/activate
```

You can go to the link above to see the Temporal UI.

3. Run `source .modal_temporal/activate` to configure your local environment to connect to Temporal in the Modal sandbox

4. Deploy Temporal worker in Modal and launch the queuer:

```bash
uv run modal_worker.py
```

5. Launch 500 workflows:

```bash
uv run launch_many_workflows.py
```

Go to the Temporal dashboard + Modal UI to see workflows completed and Modal scale up containers.

4. Clean up

```python
uv run inv stop
```
