"""Notifier service — subscribes to signal.recommendation + position.event and
sends Telegram alerts."""

from __future__ import annotations

import asyncio
import logging
import threading

from telegram import Bot
from telegram.constants import ParseMode

from shared.config import settings
from shared.redis_streams import subscribe

from formatter import (
    format_signal_alert,
    format_system_alert,
    format_position_event,
    format_marketfeed_digest,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

STREAM_SIGNAL = "signal.recommendation"
STREAM_POSITION = "position.event"
STREAM_KNOWLEDGE = "knowledge.summary"
GROUP = "notifier"


async def send_telegram(bot: Bot, text: str) -> None:
    """Send a message to the configured Telegram chat."""
    await bot.send_message(
        chat_id=settings.telegram_chat_id,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
    )
    logger.info("Sent Telegram alert (%d chars)", len(text))


async def _consume_stream(
    stream: str,
    consumer_id: str,
    formatter,
    bot: Bot | None,
) -> None:
    """Run a single Redis consumer in a worker thread and forward to Telegram."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def _reader() -> None:
        backoff = 1.0
        while True:
            try:
                for msg_id, data in subscribe(stream, group=GROUP, consumer=consumer_id, block=10_000):
                    asyncio.run_coroutine_threadsafe(queue.put((msg_id, data)), loop)
                    backoff = 1.0  # reset on successful message
            except Exception:
                logger.exception("Reader for %s crashed, retrying in %.1fs", stream, backoff)
                import time as _t
                _t.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    threading.Thread(target=_reader, daemon=True).start()

    while True:
        msg_id, data = await queue.get()
        logger.info("[%s] Received message %s", stream, msg_id)
        try:
            text = formatter(data)
            if not text:
                continue
            logger.info("[%s] Alert:\n%s", stream, text)
            if bot:
                await send_telegram(bot, text)
        except Exception:
            logger.exception("Failed to process/send %s message %s", stream, msg_id)


async def main_async() -> None:
    if not settings.telegram_bot_token:
        logger.warning("TELEGRAM_BOT_TOKEN is not set — alerts will be logged only")
        bot = None
    else:
        bot = Bot(token=settings.telegram_bot_token)
        logger.info("Telegram bot initialised")

    logger.info(
        "Notifier service starting — listening on streams: %s, %s, %s",
        STREAM_SIGNAL, STREAM_POSITION, STREAM_KNOWLEDGE,
    )

    if bot:
        try:
            await send_telegram(
                bot,
                format_system_alert(
                    "Notifier started. Listening for signals + positions + marketfeed digests."
                ),
            )
        except Exception:
            logger.exception("Failed to send startup message")

    await asyncio.gather(
        _consume_stream(STREAM_SIGNAL, "notifier-signal", format_signal_alert, bot),
        _consume_stream(STREAM_POSITION, "notifier-position", format_position_event, bot),
        _consume_stream(STREAM_KNOWLEDGE, "notifier-knowledge", format_marketfeed_digest, bot),
    )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
