import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import ReplyKeyboardBuilder


async def start_handler(message: types.Message) -> None:
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="Мой VPN")
    await message.answer(
        "Привет! Нажмите кнопку ниже.",
        reply_markup=keyboard.as_markup(resize_keyboard=True),
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    token = os.getenv("BOT_TOKEN", "8588610137:AAHg_QGoo2XpyNLkGikt6FCtmMp5iMv2WOA")
    if token == "YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Set BOT_TOKEN env var with your Telegram bot token")

    bot = Bot(token=token)
    dp = Dispatcher()

    dp.message.register(start_handler, CommandStart())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
