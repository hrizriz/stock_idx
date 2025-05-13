# import requests

# token = "7918924633:AAFyjOZVxilCo2mG1A-G-fm7buo-bJuKGC0"
# chat_id = "213349272"
# message = "Test message from Python"

# url = f"https://api.telegram.org/bot{token}/sendMessage"
# data = {"chat_id": chat_id, "text": message}

# response = requests.post(url, data=data)
# print(response.json())

# # import requests
# # from airflow.models import Variable

# # telegram_bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
# # telegram_chat_id = Variable.get("TELEGRAM_CHAT_ID")

# # url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
# # data = {"chat_id": telegram_chat_id, "text": "Test message from Airflow DAG"}

# # response = requests.post(url, data=data)
# # print(response.json())

from airflow.models import Variable

def get_telegram_credentials():
    telegram_bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = Variable.get("TELEGRAM_CHAT_ID")
    return telegram_bot_token, telegram_chat_id
