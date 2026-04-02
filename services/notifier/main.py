"""Notifier service — subscribes to signal.recommendation and sends Telegram alerts."""

from __future__ import annotations

import asyncio
import logging

from telegram import Bot
from telegram.constants import ParseMode

from shared.config import settings
from shared.redis_streams import subscribe

from formatter import format_signal_alert, format_system_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

STREAM_IN = "signal.recommendation"
GROUP = "notifier"
CONSUMER = "notifier-1"


async def send_telegram(bot: Bot, text: str) -> None:
    """Send a message to the configured Telegram chat."""
    await bot.send_message(
        chat_id=settings.telegram_chat_id,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
    )
    logger.info("Sent Telegram alert (%d chars)", len(text))


async def main_async() -> None:
    if not settings.telegram_bot_token:
        logger.warning("TELEGRAM_BOT_TOKEN is not set — alerts will be logged only")
        bot = None
    else:
        bot = Bot(token=settings.telegram_bot_token)
        logger.info("Telegram bot initialised")

    logger.info("Notifier service starting — listening on stream '%s'", STREAM_IN)

    if bot:
        try:
            await send_telegram(bot, format_system_alert("Notifier service started. Listening for signals."))
        except Exception:
            logger.exception("Failed to send startup message")

    for msg_id, data in subscribe(STREAM_IN, group=GROUP, consumer=CONSUMER, block=10_000):
        logger.info("Received recommendation message %s", msg_id)
        try:
            text = format_signal_alert(data)
            logger.info("Alert:\n%s", text)
            if bot:
                await send_telegram(bot, text)
        except Exception:
            logger.exception("Failed to process/send alert for message %s", msg_id)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
