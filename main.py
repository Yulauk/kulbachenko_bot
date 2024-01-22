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

# TELEGRAM_TOKEN = os.getenv('YOUR_TELEGRAM_BOT_TOKEN')
# MYSQL_HOST = os.getenv('DATABASE_HOST')
# MYSQL_USER = os.getenv('DATABASE_USER')
# MYSQL_PASSWORD = os.getenv('DATABASE_PASSWORD')
# MYSQL_DB = os.getenv('DATABASE_NAME')
# CHAT_ID = os.getenv('YOUR_TELEGRAM_BOT_TOKEN_CHAT_ID')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DB = os.environ.get('MYSQL_DB')
CHAT_ID = os.environ.get('CHAT_ID')

# Remove sensitive information from the log messages
logging.getLogger('httpx').setLevel(logging.WARNING)


async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f'Hello! {update.effective_user.first_name}')


async def table_free_consult_message():
    while True:
        try:
            async with aiomysql.connect(
                    host=os.getenv('MYSQL_HOST'),
                    user=os.getenv('MYSQL_USER'),
                    password=os.getenv('MYSQL_PASSWORD'),
                    db=os.getenv('MYSQL_DB')) as conn:
                async with conn.cursor() as cursor:
                    try:
                        # Get the maximum ID
                        await cursor.execute("SELECT MAX(id) FROM free_consult;")
                        max_id = await cursor.fetchone()
                        if max_id and max_id[0] is not None:
                            max_id = max_id[0]

                            await asyncio.sleep(60)

                            # Check for new records
                            await cursor.execute(f"SELECT * FROM free_consult WHERE id>{max_id};")
                            lst_message = [i for sub in await cursor.fetchall() for i in sub]
                            if lst_message:
                                message = (f'NEW *free consult*\n'
                                           f'id:{lst_message[0]}\n'
                                           f'name: {lst_message[1]}\n'
                                           f'contact: {lst_message[2]}\n'
                                           f'comment: {lst_message[3]}\n'
                                           f'date: {lst_message[4].isoformat()}')
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
                    host=os.getenv('MYSQL_HOST'),
                    user=os.getenv('MYSQL_USER'),
                    password=os.getenv('MYSQL_PASSWORD'),
                    db=os.getenv('MYSQL_DB')) as conn:
                async with conn.cursor() as cursor:
                    try:
                        # Get the maximum ID
                        await cursor.execute("SELECT MAX(id) FROM contact;")
                        max_id = await cursor.fetchone()
                        if max_id:
                            max_id = max_id[0]

                            await asyncio.sleep(60)

                            # Check for new records
                            if max_id is not None:
                                await cursor.execute(f"SELECT * FROM contact WHERE id>{max_id};")
                                lst_message = [i for sub in await cursor.fetchall() for i in sub]
                                if lst_message:
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

    try:
        # Run the bot and all continuous functions concurrently
        app.run_polling()
        # print(f'>>{MYSQL_HOST}\n>>{MYSQL_DB}')
        # Wait for all tasks to complete before exiting
        loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        # Handle KeyboardInterrupt to stop the application gracefully
        pass
    finally:
        # Close the event loop
        loop.close()
