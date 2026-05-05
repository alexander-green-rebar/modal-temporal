import hashlib
import urllib.request

from temporalio import activity


@activity.defn
async def fetch_bytes(url: str) -> bytes:
    """Fetch URL contents."""
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read()


@activity.defn
async def hash_bytes(data: bytes) -> str:
    """Compute SHA-256 hex digest."""
    return hashlib.sha256(data).hexdigest()
