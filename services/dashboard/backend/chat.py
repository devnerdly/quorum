"""Chat service — Anthropic-powered streaming assistant for the trading dashboard."""

from __future__ import annotations

import json
import logging
from typing import Generator

from anthropic import Anthropic

from shared.config import settings
from shared.redis_streams import get_redis
from chat_tools import TOOLS, execute_tool

logger = logging.getLogger(__name__)

MODEL = "claude-opus-4-6"

SYSTEM_PROMPT = """You are the trading assistant for a Brent crude oil CFD trader on XTB.
You have access to a live trading bot's database (5-component scoring, AI-generated recommendations,
@marketfeed digests, open positions).

Today's date is 2026-04-08. The user trades intraday on 15-min cycles.

CRITICAL RULES:
1. ALWAYS call get_current_market_state FIRST when the user asks anything about current state, "should I", "what now", or trading decisions. NEVER guess prices from training data.
2. CITE specific evidence — recommendation IDs, knowledge digest events, score values.
3. Be concise (3-5 sentences max in normal answers; tables for data).
4. If the user asks "should I long/short now", run get_current_market_state + query_marketfeed(hours=2) + get_open_positions, then give a clear recommendation with reasoning.
5. Never invent data. If a tool returns None / empty, say so."""


# ---------------------------------------------------------------------------
# Session history helpers (Redis-backed, 24 h TTL)
# ---------------------------------------------------------------------------

def _history_key(session_id: str) -> str:
    return f"chat:{session_id}"


def _load_history(session_id: str) -> list[dict]:
    r = get_redis()
    raw = r.get(_history_key(session_id))
    if raw is None:
        return []
    try:
        return json.loads(raw)
    except Exception:
        return []


def _save_history(session_id: str, history: list[dict]) -> None:
    r = get_redis()
    r.setex(_history_key(session_id), 86400, json.dumps(history))


# ---------------------------------------------------------------------------
# Streaming chat generator
# ---------------------------------------------------------------------------

def stream_chat(message: str, session_id: str = "default") -> Generator[str, None, None]:
    """Generator yielding SSE-formatted event strings."""
    history = _load_history(session_id)
    history.append({"role": "user", "content": message})

    client = Anthropic(api_key=settings.anthropic_api_key)

    # Agentic tool-use loop — continue until the model stops requesting tools
    max_iterations = 20
    for iteration in range(max_iterations):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=1500,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=history,
            )
        except Exception as exc:
            logger.exception("Anthropic API call failed (iteration %d)", iteration)
            yield f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n"
            return

        # Decompose the response into text blocks and tool_use blocks
        assistant_blocks: list[dict] = []
        text_content = ""
        tool_calls: list[dict] = []

        for block in response.content:
            if block.type == "text":
                text_content += block.text
                assistant_blocks.append({"type": "text", "text": block.text})
            elif block.type == "tool_use":
                tool_calls.append(
                    {"id": block.id, "name": block.name, "input": block.input}
                )
                assistant_blocks.append(
                    {
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    }
                )

        # Append the full assistant turn (with tool_use blocks) to history
        history.append({"role": "assistant", "content": assistant_blocks})

        # Stream the text portion to the client
        if text_content:
            yield f"event: token\ndata: {json.dumps({'text': text_content})}\n\n"

        # Stream tool-call metadata so the UI can show "thinking" indicators
        for tc in tool_calls:
            yield (
                f"event: tool_call\ndata: "
                f"{json.dumps({'name': tc['name'], 'input': tc['input']})}\n\n"
            )

        # If the model is done (no tool calls or stop_reason != tool_use), wrap up
        if not tool_calls or response.stop_reason != "tool_use":
            _save_history(session_id, history)
            yield f"event: done\ndata: {json.dumps({})}\n\n"
            return

        # Execute each requested tool and collect results
        tool_results: list[dict] = []
        for tc in tool_calls:
            try:
                result = execute_tool(tc["name"], tc["input"])
                yield (
                    f"event: tool_result\ndata: "
                    f"{json.dumps({'name': tc['name'], 'output': result}, default=str)}\n\n"
                )
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tc["id"],
                        # Truncate at 8 000 chars to stay within context budget
                        "content": json.dumps(result, default=str)[:8000],
                    }
                )
            except Exception as exc:
                logger.exception("Tool '%s' raised an exception", tc["name"])
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tc["id"],
                        "content": f"error: {exc}",
                        "is_error": True,
                    }
                )

        # Feed tool results back as a user turn so the model can continue
        history.append({"role": "user", "content": tool_results})

    # Safety net: should never reach here in practice
    logger.error("stream_chat exceeded max_iterations=%d for session %s", max_iterations, session_id)
    yield f"event: error\ndata: {json.dumps({'error': 'max iterations exceeded'})}\n\n"
