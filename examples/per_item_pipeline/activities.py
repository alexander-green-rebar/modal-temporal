import re
import urllib.request

from temporalio import activity


@activity.defn
async def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return resp.read().decode("utf-8", errors="replace")


@activity.defn
async def word_count(text: str) -> int:
    return len(re.findall(r"\b[a-zA-Z]+\b", text))


@activity.defn
async def excerpt(text: str, max_chars: int) -> str:
    stripped = re.sub(r"<[^>]+>", " ", text)
    collapsed = re.sub(r"\s+", " ", stripped).strip()
    suffix = "…" if len(collapsed) > max_chars else ""
    return collapsed[:max_chars] + suffix
