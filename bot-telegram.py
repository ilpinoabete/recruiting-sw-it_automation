from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv
from io import StringIO
import requests as r
import csv
import os

load_dotenv()

# List of allowed users to use the bot
ALLOWED_USERS = eval(os.getenv("ALLOWED_USERS"))
# Authentication header for NocoDB API
AUTH_HEADER = {"xc-token": os.getenv("DB_KEY")}

def generate_csv(data: list[dict]) -> StringIO:
    """
    Converts a list of dictionaries to a CSV file stored in memory (StringIO).
    Returns the in-memory file object.

    Args:
        data (list[dict]): List of dictionaries.

    Returns:
        StringIO: CSV file.
    """

    # Basic validation
    if not data:
        raise ValueError("No data provided to generate CSV.")

    # Create in-memory CSV file
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    output.seek(0) # Move to the beginning of the StringIO object

    return output

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Send a message when the command /start is issued.
    """

    await update.message.reply_text("Hi, I am an useful bot that can retrieve data from your NocoDB installation!\n"
                                    "The command that i support is /list, to list the items of a certain table"
                                    )

async def list_tables(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Retrieves the available tables from the NocoDB installation and lets the user select which one to show the information of.
    """

    # Check if the user is allowed to use the bot
    if update.message.chat.username in ALLOWED_USERS:

        # Retrieve the list of existing tables
        tables = r.get(
            f"http://{os.getenv('SERVER_URL')}/api/v2/meta/bases/{os.getenv('BASE_ID')}/tables",
            headers=AUTH_HEADER,
        ).json()

        tables = tables["list"]
        keyboard = []

        # Crating the keyboard with the available tables
        for i in range(len(tables)):
            keyboard.append([
                InlineKeyboardButton(
                    tables[i]['table_name'],
                    callback_data=str(
                        {"table_name" : tables[i]['table_name'], "table_id" : tables[i]['id']}
                    )
                )
            ])

        reply_markup = InlineKeyboardMarkup(keyboard)

        # Asking the user which table to show
        await update.message.reply_text("Which table do you want to list the content of?", reply_markup=reply_markup)

    else:
        await update.message.reply_text("Sorry, you are not allowed to use this command.")

async def list_items(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Receives the selected table id from the `list_tables` function and retrieves its content.
    Then it calls the `generate_csv` function and returns the CSV file to the user
    """

    # Acknowledge the callback query
    query = update.callback_query
    await query.answer()

    # Retrieve the selected table content
    table_content = r.get(
        f"http://{os.getenv('SERVER_URL')}/api/v2/tables/{eval(query.data)['table_id']}/records",
        headers=AUTH_HEADER
    ).json()

    table_content = table_content["list"]
    csv_file = generate_csv(table_content)

    # Sending the CSV file to the user
    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=csv_file,
        filename=f"{eval(query.data)['table_name']}.csv"
    )

    del csv_file

def main() -> None:
    # Create the bot with the environment API key
    bot = Application.builder().token(os.getenv("TELEGRAM_API_KEY")).build()

    # Adding handlers
    bot.add_handler(CommandHandler("start", start))
    bot.add_handler(CommandHandler("list", list_tables))
    bot.add_handler(CallbackQueryHandler(list_items))

    # Keep the bot running
    bot.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        exit(0)

    except Exception as e:
        print(e)
