#!/bin/python

import os
import sys
import argparse
import asyncio
import yaml
import importlib

from pathlib import Path
from dataclasses import dataclass

from temporalio.client import Client
from temporalio.worker import Worker

from foundry.common import logger

@dataclass
class TemporalConfig:
    server_url: str = os.environ["TEMPORAL_SERVER_URL"]
    api_key = os.environ["TEMPORAL_API_KEY"]
    namespace = os.environ["TEMPORAL_NAMESPACE"]

    path: Path = Path("/temporal")

    def resolve_config_path(self, filename:str) -> Path:
        return self.path / f"configs/{filename}"


async def get_client(temporal_config:TemporalConfig):
    return await Client.connect(
        target_host=temporal_config.server_url,
        api_key=temporal_config.api_key,
        namespace=temporal_config.namespace,
        tls=True,
    )


async def run_worker(config_filename: str = "default.yml"):
    temporal_config = TemporalConfig()

    logger.info(f"connecting to {temporal_config.server_url}")
    client = await get_client(temporal_config)

    # Load workflow and activities configuration
    with temporal_config.resolve_config_path(config_filename).open() as f:
        config = yaml.safe_load(f)

    # Workflows and activities are in a nested path
    if str(temporal_config.path) not in sys.path:
        sys.path.insert(0, str(temporal_config.path))
    
    # Load workflows and activities from config
    workflows = []
    activities = []
    
    for workflow_config in config.get("workflows", []):
        module = importlib.import_module(workflow_config["module"])
        workflow_class = getattr(module, workflow_config["class"])
        workflows.append(workflow_class)
    
    for activity_config in config.get("activities", []):
        module = importlib.import_module(activity_config["module"])
        activity_func = getattr(module, activity_config["function"])
        activities.append(activity_func)
    
    # Start worker
    worker = Worker(
        client,
        task_queue="example-task-queue",
        workflows=workflows,
        activities=activities,
    )

    logger.info("starting worker")    
    await worker.run()


async def start_workflow(workflow:str):
    temporal_config = TemporalConfig()
    client = await get_client(temporal_config)
    
    # Start your workflow
    result = await client.execute_workflow(
        workflow,
        id="test-workflow-1",
        task_queue="example-task-queue"
    )
    
    print(f"Workflow result: {result}")


def main():
    parser = argparse.ArgumentParser(description="Run configurable Temporal worker or start workflow")
    parser.add_argument(
        "--mode",
        "-m", 
        type=str,
        choices=["worker", "workflow"],
        default="worker",
        help="Mode to run: 'worker' to start worker, 'workflow' to execute test workflow (default: worker)"
    )
    parser.add_argument(
        "--workflow", 
        "-w",
        type=str, 
        help="Name of workflow to execute"
    )
    parser.add_argument(
        "--config", 
        "-c",
        type=str, 
        default="default.yml",
        help="Path to the worker configuration YAML file (default: default.yml)"
    )
    
    args = parser.parse_args()
    
    if args.mode == "worker":
        asyncio.run(run_worker(args.config))
    elif args.mode == "workflow":
        asyncio.run(start_workflow(args.workflow))


if __name__ == "__main__":
    main()
    