# Modal Temporal Demo

## Instructions

0. Clone this branch

```bash
git clone -b --single-branch thomasjpfan/native-worker https://github.com/modal-projects/modal-temporal
cd
```

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/)

2. Deploy temporal server in a Modal Sandbox. (Only used for testing)

```bash
uv run inv develop
```

which should output:

```
🚀 Temporal started on Modal!
🌎 Temporal UI: ...
💻 Configure your local environment by running:

source .modal_temporal/activate
```

3. Run `source .modal_temporal/activate` to configure your local environment to connect to the sandbox in temporal

4. Deploy Temporal worker in Modal and launch the queuer:

```bash
uv run modal_worker.py
```

5. Launch 500 workflows:

```bash
uv run launch_many_workflows.py
```

4. Clean up

```python
uv run inv stop
```
