from telegram import Update
from telegram.ext import (
    CommandHandler,
    Application,
    ContextTypes
)
import logging
import requests
from dotenv import load_dotenv
import os


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv("TOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """The handler of the command /start"""
    await update.message.reply_text("Бот запущен!")

def send_telegram_message(user_id: int, message_text: str) -> bool:
    """
    The function of sending message by bot in Telegram

    Parameters
    ---------
    user_id : int
        unique ID of telegram user to whom the message should be sent

    message_test : str
        the text that will be sent to the user

    Returns
    -------
    bool
        parameter indicating the success of sending a message
    """
    api_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": user_id,
        "text": message_text
    }
    response = requests.post(api_url, json=data)
    return response.status_code == 200

def main():
    """Launching the bot"""
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))

    application.run_polling()

if __name__ == "__main__":
    main()