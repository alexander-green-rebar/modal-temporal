import re
from temporalio import activity


@activity.defn
async def greet(name: str) -> str:
    return f"Hello {name}"


@activity.defn
def word_count(text: str) -> int:
    return len(re.findall(r"\b[a-zA-Z]+\b", text))
