import os
import logging
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import aiomysql
import asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

TELEGRAM_TOKEN = os.getenv('YOUR_TELEGRAM_BOT_TOKEN')
MYSQL_HOST = os.getenv('DATABASE_HOST')
MYSQL_USER = os.getenv('DATABASE_USER')
MYSQL_PASSWORD = os.getenv('DATABASE_PASSWORD')
MYSQL_DB = os.getenv('DATABASE_NAME')
CHAT_ID = os.getenv('YOUR_TELEGRAM_BOT_TOKEN_CHAT_ID')

# Remove sensitive information from the log messages
logging.getLogger('httpx').setLevel(logging.WARNING)


async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f'Hello! {update.effective_user.first_name}')


async def table_free_consult_message():
    while True:
        try:
            async with aiomysql.connect(
                    host=os.getenv('DATABASE_HOST'),
                    user=os.getenv('DATABASE_USER'),
                    password=os.getenv('DATABASE_PASSWORD'),
                    db=os.getenv('DATABASE_NAME')) as conn:
                async with conn.cursor() as cursor:
                    try:
                        await cursor.execute("SELECT * FROM free_consult;")
                        records = await cursor.fetchall()
                        len_records = len(records)

                        await asyncio.sleep(60)
                        await cursor.execute("SELECT * FROM free_consult;")
                        records = await cursor.fetchall()

                        if len(records) > len_records:
                            await cursor.execute(f"SELECT * FROM free_consult WHERE id>{len_records};")
                            new_records = await cursor.fetchall()
                            lst_message = [i for sub in new_records for i in sub]

                            message = (f'NEW *free consult*\n'
                                       f'id:{lst_message[0]}\n'
                                       f'name: {lst_message[1]}\n'
                                       f'contact: {lst_message[2]}\n'
                                       f'comment: {lst_message[3]}\n'
                                       f'date: {lst_message[4].isoformat()}')
                            print('>>>', message)
                            user_id = CHAT_ID
                            await app.bot.send_message(chat_id=user_id, text=message)

                    except aiomysql.Error as query_err:
                        logger.error(f"Query Error: {query_err}")
        except aiomysql.Error as conn_err:
            logger.error(f"Connection Error: {conn_err}")


async def table_contact_message():
    while True:
        try:
            async with aiomysql.connect(
                    host=os.getenv('DATABASE_HOST'),
                    user=os.getenv('DATABASE_USER'),
                    password=os.getenv('DATABASE_PASSWORD'),
                    db=os.getenv('DATABASE_NAME')) as conn:
                async with conn.cursor() as cursor:
                    try:
                        await cursor.execute("SELECT * FROM contact;")
                        records = await cursor.fetchall()
                        len_records = len(records)

                        await asyncio.sleep(60)
                        await cursor.execute("SELECT * FROM contact;")
                        records = await cursor.fetchall()

                        if len(records) > len_records:
                            await cursor.execute(f"SELECT * FROM contact WHERE id>{len_records};")
                            new_records = await cursor.fetchall()
                            lst_message = [i for sub in new_records for i in sub]

                            message = (f'NEW {"*CONTACT US*"}\n'
                                       f'id:{lst_message[0]}\n'
                                       f'name: {lst_message[1]}\n'
                                       f'surname: {lst_message[2]}\n'
                                       f'email: {lst_message[3]}\n'
                                       f'phone: {lst_message[4]}\n'
                                       f'city: {lst_message[5]}\n'
                                       f'state: {lst_message[6]}\n'
                                       f'zip: {lst_message[7]}\n'
                                       f'address: {lst_message[8]}\n'
                                       f'budget: {lst_message[9]}\n'
                                       f'time: {lst_message[10]}\n'
                                       f'source: {lst_message[11]}\n'
                                       f'project: {lst_message[12]}\n'
                                       f'date: {lst_message[13].isoformat()}')
                            user_id = CHAT_ID
                            await app.bot.send_message(chat_id=user_id, text=message)

                    except aiomysql.Error as query_err:
                        logger.error(f"Query Error: {query_err}")
        except aiomysql.Error as conn_err:
            logger.error(f"Connection Error: {conn_err}")


app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

# commands
app.add_handler(CommandHandler("hello", hello))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Create tasks for all continuous functions
    tasks = [
        loop.create_task(table_free_consult_message()),
        loop.create_task(table_contact_message()),
    ]

    # Run the bot and all continuous functions concurrently
    app.run_polling()

    # Wait for all tasks to complete before exiting
    loop.run_until_complete(asyncio.gather(*tasks))
