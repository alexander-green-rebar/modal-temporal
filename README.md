# Temporal + Modal

This repository contains an example of how to integrate Temporal
and Modal using [Sandboxes](https://modal.com/docs/guide/sandbox#sandboxes).
We create a Modal App that launches Temporal Workers. These workers serve
as the compute layer for running Temporal Workloads.

## Usage

### Step 1: set local env vars

Rename `.env.example` to `.env`. Then populate `.env.example` with required
environment variableS:

```env
TEMPORAL_SERVER_URL=
TEMPORAL_API_KEY=
TEMPORAL_NAMESPACE=
```
Then make them available in your environment with:

```shell
export $(cat .env)
```

### Step 2: create Modal Secret

Now create a Modal Secret called `temporal-secrets` with those three
environment variable. 

### Step 3: sync files

Create Volume and sync workflow, activity, and config files with:

```shell
python sync_files.py
```

This will store files from the following directories into a Modal Volume:

- `/workflows`: defines Temporal Workflows
- `/activities`: defines Temporal Activities (aka functions)
- `/configs`: groups Workflows and Activities in a config

Configs are designed to configure workers to start only with a set
of workflows.


### Step 4: deploy Modal app

The Modal App contains (A) a function to build images using `conda` to manage
environments and (B) a scheduled Modal Function that builds images automatically 
(this is designed to observe a database and build images that match a `conda`
environment).

```shell
modal deploy deploy.py
```

### Step 5: run example

Run the example script with:

```shell
python run_example.py
```

This will

1. Build an image, using `conda` to install dependencies
2. Start a Temporal Worker with the image ID above
3. Trigger a Temporal Workflow, which runs in the Worker above
