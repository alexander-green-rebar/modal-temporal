from datetime import timedelta
from typing import TypedDict

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import excerpt, fetch_text, word_count


class Summary(TypedDict):
    url: str
    words: int
    excerpt: str


@workflow.defn
class AnalyzeUrl:
    @workflow.run
    async def run(self, url: str) -> Summary:
        # fetch_text gets a per-workflow retry policy — bounded, so unreachable
        # URLs fail fast instead of retrying forever.
        text = await workflow.execute_activity(
            fetch_text,
            url,
            schedule_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_attempts=2,
            ),
        )
        wc = await workflow.execute_activity(
            word_count,
            text,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        ex = await workflow.execute_activity(
            excerpt,
            args=[text, 80],
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        return Summary(url=url, words=wc, excerpt=ex)
