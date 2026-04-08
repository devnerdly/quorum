"""Adversarial trading committee: Bull vs Bear vs Judge.

Two sub-agents (Claude Sonnet) argue opposite sides of the same Brent crude
setup using the same pre-fetched market context. A judge (Claude Opus) then
reads both cases and renders a final verdict with specific action and levels.

Reduces confirmation bias and hallucination — the model can't just pick a
comfortable answer because another instance is actively defending the opposite.
"""

from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from anthropic import Anthropic

from shared.config import settings

logger = logging.getLogger(__name__)

BULL_BEAR_MODEL = "claude-sonnet-4-6"
JUDGE_MODEL = "claude-opus-4-6"

BULL_SYSTEM = """You are a permabull oil trader.

Your job: build the STRONGEST POSSIBLE bullish case for Brent crude given the data below.
Find every reason to go LONG:
  - Supply disruption risks (geopolitics, sanctions, weather, infrastructure damage)
  - OPEC+ discipline / production cuts
  - Inventory draws below expectations
  - Demand tailwinds (China stimulus, seasonal driving, industrial activity)
  - Technical breakouts, higher lows, support holding
  - Bullish sentiment shifts on Twitter/marketfeed
  - USD weakness (cheaper for non-USD buyers)
  - Refinery margin expansion

Rules:
  - Cite SPECIFIC data from the context (scores, price levels, digest summaries).
  - If the bullish case is weak, say so honestly in 'case_strength' but still make the best argument you can.
  - Never invent data — use only what's in the context.

Return ONLY a JSON object (no markdown, no preamble) matching this schema:
{
  "side": "LONG",
  "thesis": "1-2 sentence core thesis",
  "key_arguments": ["3-5 specific bullish factors, each citing data"],
  "strongest_evidence": "the single most compelling piece of evidence",
  "price_targets": {"entry": <float|null>, "tp": <float|null>, "sl": <float|null>},
  "risks_to_thesis": ["2-3 things that would invalidate the bull case"],
  "confidence": <float 0.0 to 1.0>,
  "case_strength": "strong" | "moderate" | "weak"
}"""

BEAR_SYSTEM = """You are a permabear oil trader.

Your job: build the STRONGEST POSSIBLE bearish case for Brent crude given the data below.
Find every reason to go SHORT:
  - Demand destruction (recession, China slowdown, EV penetration, efficiency gains)
  - Oversupply (OPEC+ unwinding, US shale, non-OPEC growth)
  - Inventory builds above expectations
  - USD strength headwinds (stronger dollar = weaker oil)
  - Hawkish Fed, tightening credit
  - Technical breakdowns, lower highs, resistance holding
  - De-escalation narratives removing war premium
  - Refinery margin compression

Rules:
  - Cite SPECIFIC data from the context (scores, price levels, digest summaries).
  - If the bearish case is weak, say so honestly in 'case_strength' but still make the best argument you can.
  - Never invent data — use only what's in the context.

Return ONLY a JSON object (no markdown, no preamble) matching this schema:
{
  "side": "SHORT",
  "thesis": "1-2 sentence core thesis",
  "key_arguments": ["3-5 specific bearish factors, each citing data"],
  "strongest_evidence": "the single most compelling piece of evidence",
  "price_targets": {"entry": <float|null>, "tp": <float|null>, "sl": <float|null>},
  "risks_to_thesis": ["2-3 things that would invalidate the bear case"],
  "confidence": <float 0.0 to 1.0>,
  "case_strength": "strong" | "moderate" | "weak"
}"""

JUDGE_SYSTEM = """You are the chief strategist presiding over an adversarial trading committee.

Two subordinate agents (Bull and Bear) have each built the strongest case for their side
using the same market context. Your job: evaluate both cases on the merits of the evidence,
NOT on which side you personally prefer, and render a final verdict.

Guidelines:
  - Weight STRENGTH OF EVIDENCE over rhetorical conviction.
  - If one side's 'strongest_evidence' is a specific recent event and the other is a general thesis,
    the specific event usually wins for short-horizon trades.
  - If both sides have strong evidence, WAIT is better than flipping a coin.
  - Always consider the user's existing open campaigns (if any) — don't recommend opening
    against an existing same-direction position, and flag conflicts with opposite positions.
  - Be decisive when the evidence clearly favors one side.

Return ONLY a JSON object (no markdown, no preamble):
{
  "action": "ENTER_LONG" | "ENTER_SHORT" | "WAIT" | "AVOID" | "MANAGE_EXISTING",
  "winning_side": "BULL" | "BEAR" | "NEITHER",
  "conviction_score": <float -100 to +100, negative=bear, positive=bull>,
  "confidence": <float 0.0 to 1.0>,
  "rationale": "2-4 sentences explaining the decision",
  "key_pros": ["2-3 reasons supporting the verdict"],
  "key_cons": ["2-3 risks to the verdict"],
  "specific_action": "concrete next step: entry level, SL, TP, or 'wait for X event'",
  "bull_rating": <float 0-10>,
  "bear_rating": <float 0-10>
}"""


_client: Anthropic | None = None


def _get_client() -> Anthropic:
    global _client
    if _client is None:
        _client = Anthropic(api_key=settings.anthropic_api_key)
    return _client


def _strip_json(text: str) -> str:
    """Strip markdown fences and surrounding prose around a JSON object."""
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence:
        text = fence.group(1).strip()
    if not text.startswith("{"):
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            text = m.group(0)
    text = re.sub(r",\s*([\]\}])", r"\1", text)
    return text


def _fetch_context(focus_hours: int) -> dict:
    """Pre-fetch the same market context for both agents. No LLM calls here."""
    context: dict = {}

    try:
        from chat_tools import _get_current_market_state
        context["market"] = _get_current_market_state()
    except Exception as exc:
        context["market"] = {"error": str(exc)}

    try:
        from chat_tools import _query_marketfeed
        context["news"] = _query_marketfeed(hours=focus_hours)
    except Exception as exc:
        context["news"] = {"error": str(exc)}

    try:
        from plugin_analytics import _get_support_resistance, _get_vwap, _get_upcoming_events
        context["support_resistance"] = _get_support_resistance(timeframe="1H", lookback_bars=100)
        context["vwap"] = _get_vwap(timeframe="1H", hours=24)
        context["upcoming_events"] = _get_upcoming_events(days=2)
    except Exception as exc:
        logger.exception("analytics sub-tool failed in committee context fetch")
        context["analytics_error"] = str(exc)

    return context


def _run_agent(system_prompt: str, context: dict, label: str) -> dict:
    """Run a single Sonnet agent with the given system prompt and context."""
    user_prompt = (
        f"## Market Context (authoritative — do not invent prices)\n"
        f"{json.dumps(context, indent=2, default=str)[:12000]}\n\n"
        f"Build your {label} case now. Return ONLY the JSON object."
    )

    try:
        response = _get_client().messages.create(
            model=BULL_BEAR_MODEL,
            max_tokens=1200,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = response.content[0].text if response.content else ""
        cleaned = _strip_json(raw)
        return json.loads(cleaned)
    except Exception as exc:
        logger.exception("%s agent failed", label)
        return {"error": f"{label} agent failed: {exc}"}


def _run_judge(context: dict, bull_case: dict, bear_case: dict) -> dict:
    """Run the judge with both cases + the original context."""
    user_prompt = (
        f"## Market Context\n{json.dumps(context, indent=2, default=str)[:8000]}\n\n"
        f"## Bull Case\n{json.dumps(bull_case, indent=2)}\n\n"
        f"## Bear Case\n{json.dumps(bear_case, indent=2)}\n\n"
        "Render your verdict now. Return ONLY the JSON object."
    )

    try:
        response = _get_client().messages.create(
            model=JUDGE_MODEL,
            max_tokens=1500,
            system=JUDGE_SYSTEM,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = response.content[0].text if response.content else ""
        cleaned = _strip_json(raw)
        return json.loads(cleaned)
    except Exception as exc:
        logger.exception("Judge failed")
        return {"error": f"judge failed: {exc}"}


def _committee_debate(focus_hours: int = 4) -> dict:
    """Run a full adversarial committee debate and return structured verdict."""
    started = datetime.now(tz=timezone.utc)

    context = _fetch_context(focus_hours=focus_hours)

    bull_result: dict = {}
    bear_result: dict = {}

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(_run_agent, BULL_SYSTEM, context, "BULL"): "bull",
            executor.submit(_run_agent, BEAR_SYSTEM, context, "BEAR"): "bear",
        }
        for future in as_completed(futures):
            label = futures[future]
            try:
                if label == "bull":
                    bull_result = future.result(timeout=60)
                else:
                    bear_result = future.result(timeout=60)
            except Exception as exc:
                logger.exception("Committee agent %s exploded", label)
                if label == "bull":
                    bull_result = {"error": str(exc)}
                else:
                    bear_result = {"error": str(exc)}

    judge_result = _run_judge(context, bull_result, bear_result)

    ended = datetime.now(tz=timezone.utc)
    duration_seconds = (ended - started).total_seconds()

    return {
        "started_at": started.isoformat(),
        "duration_seconds": round(duration_seconds, 1),
        "context_summary": {
            "current_price": (context.get("market") or {}).get("current_price"),
            "unified_score": ((context.get("market") or {}).get("scores") or {}).get("unified"),
            "news_count": ((context.get("news") or {}).get("count")),
            "open_campaigns": ((context.get("market") or {}).get("account") or {}).get("open_campaigns"),
        },
        "bull_case": bull_result,
        "bear_case": bear_result,
        "judge_verdict": judge_result,
    }


# ---------------------------------------------------------------------------
# Plugin API
# ---------------------------------------------------------------------------

PLUGIN_TOOLS: list[dict] = [
    {
        "name": "committee_debate",
        "description": (
            "Run an adversarial trading committee: a Bull agent and a Bear agent each build "
            "the strongest case for their side using the same market context, then a Judge "
            "reads both cases and renders a final verdict with specific action and levels. "
            "Use when the user asks for a debate, a second opinion, adversarial analysis, "
            "'let them argue', or when scores are conflicting and a single view isn't enough. "
            "Costs ~3 LLM calls (2 Sonnet + 1 Opus) and takes ~15-20 seconds."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "focus_hours": {
                    "type": "integer",
                    "default": 4,
                    "description": "How many hours of news/context to pull for both agents",
                },
            },
        },
    }
]


def execute(name: str, tool_input: dict) -> dict | None:
    if name == "committee_debate":
        return _committee_debate(**tool_input)
    return None
