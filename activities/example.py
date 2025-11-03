from temporalio import activity


@activity.defn
async def example_activity() -> None:
    print("hello")
