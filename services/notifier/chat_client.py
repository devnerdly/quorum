"""Streaming HTTP client that talks to the dashboard chat endpoint."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import AsyncIterator

import httpx

logger = logging.getLogger(__name__)

# Inside the docker network the dashboard is reachable by service name.
DASHBOARD_CHAT_URL = "http://dashboard:8000/api/chat"
REQUEST_TIMEOUT = 240.0  # Opus + tools loop can take a while


@dataclass
class ChatEvent:
    """One streaming event from the chat backend."""
    kind: str  # "token" | "tool_call" | "tool_result" | "done" | "error"
    name: str | None = None       # tool name (for tool_call / tool_result)
    text: str | None = None       # token text
    error: str | None = None
    output: dict | None = None    # tool_result output


@dataclass
class ChatProgress:
    """Mutable progress state. The notifier renders this as the placeholder is edited."""
    text: str = ""
    tool_calls: list[str] = field(default_factory=list)  # in order, may have duplicates
    tool_results: dict[str, dict] = field(default_factory=dict)  # name -> last output
    finished: bool = False
    error: str | None = None


async def _iter_sse_events(response: httpx.Response) -> AsyncIterator[ChatEvent]:
    """Yield ChatEvent objects from an SSE response stream."""
    current_event = "message"
    async for line in response.aiter_lines():
        if not line:
            current_event = "message"
            continue
        if line.startswith("event:"):
            current_event = line[6:].strip()
        elif line.startswith("data:"):
            payload = line[5:].strip()
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning("SSE payload not JSON: %r", payload)
                continue

            if current_event == "token":
                yield ChatEvent(kind="token", text=data.get("text", ""))
            elif current_event == "tool_call":
                yield ChatEvent(kind="tool_call", name=data.get("name") or "?")
            elif current_event == "tool_result":
                yield ChatEvent(
                    kind="tool_result",
                    name=data.get("name") or "?",
                    output=data.get("output") if isinstance(data.get("output"), dict) else None,
                )
            elif current_event == "done":
                yield ChatEvent(kind="done")
            elif current_event == "error":
                yield ChatEvent(kind="error", error=str(data.get("error", "unknown")))


async def chat_stream(message: str, session_id: str) -> AsyncIterator[ChatEvent]:
    """Stream events from the dashboard /api/chat SSE endpoint as they arrive."""
    payload = {"message": message, "session_id": session_id}

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            async with client.stream("POST", DASHBOARD_CHAT_URL, json=payload) as response:
                if response.status_code != 200:
                    body = await response.aread()
                    yield ChatEvent(
                        kind="error",
                        error=f"HTTP {response.status_code}: {body.decode('utf-8', errors='replace')[:300]}",
                    )
                    return

                async for event in _iter_sse_events(response):
                    yield event
                    if event.kind in ("done", "error"):
                        return
    except httpx.RequestError as exc:
        yield ChatEvent(kind="error", error=f"connection error: {exc}")
    except Exception as exc:
        logger.exception("chat_stream failed unexpectedly")
        yield ChatEvent(kind="error", error=f"unexpected error: {exc}")


# ---------------------------------------------------------------------------
# Progress rendering for the Telegram placeholder
# ---------------------------------------------------------------------------

# Friendly emoji prefix per tool name
_TOOL_ICONS = {
    "get_current_market_state": "\U0001f4ca",  # bar chart
    "get_price_history":        "\U0001f4c8",  # chart up
    "query_marketfeed":         "\U0001f4f0",  # newspaper
    "get_signal_detail":        "\U0001f4cb",  # clipboard
    "get_recent_signals":       "\U0001f4dc",  # scroll
    "get_open_positions":       "\U0001f4bc",  # briefcase
    "get_account_state":        "\U0001f4b0",  # money bag
    "get_campaigns":            "\U0001f5c2\ufe0f",  # card index
    "get_campaign_detail":      "\U0001f5d2\ufe0f",  # spiral notepad
    "simulate_trade":           "\U0001f9ee",  # abacus
    "close_campaign":           "\u274c",     # red X
    "add_dca_layer":            "\u2795",     # heavy plus
    "open_new_campaign":        "\U0001f680",  # rocket
}


def render_progress(progress: ChatProgress) -> str:
    """Render the in-progress chat state into a Telegram-safe message."""
    lines: list[str] = []

    if progress.tool_calls:
        # Show tool history with check marks for completed ones
        seen: set[str] = set()
        unique = [t for t in progress.tool_calls if not (t in seen or seen.add(t))]
        lines.append("*\U0001f527 Researching…*")
        for name in unique:
            icon = _TOOL_ICONS.get(name, "\u2022")
            done = name in progress.tool_results
            check = "\u2705" if done else "\u23f3"
            lines.append(f"  {check} {icon} `{name}`")
        lines.append("")

    if progress.text:
        lines.append(progress.text.strip())
    elif not progress.finished:
        lines.append("_Thinking…_")

    if progress.error:
        lines.append(f"\n\u274c *Error*: {progress.error}")

    return "\n".join(lines).strip() or "(empty)"
