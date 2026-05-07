from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import greet
    from activities import word_count


@workflow.defn
class SayHelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> int:
        result = await workflow.execute_activity(
            greet,
            name,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        count = await workflow.execute_activity(
            word_count,
            result,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        return count
