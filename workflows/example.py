from datetime import timedelta
from temporalio import workflow

from activities.example import example_activity


@workflow.defn
class ExampleWorkflow:
    """Simple data processing workflow"""
    
    @workflow.run
    async def run(self) -> str:
        await workflow.execute_activity(
            example_activity,
            start_to_close_timeout=timedelta(minutes=1)
        )
        
        return "ExampleWorkflow is now done"

