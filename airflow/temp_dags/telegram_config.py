# telegram_config.py
# Credential untuk Telegram
TELEGRAM_BOT_TOKEN = "7918924633:AAFyjOZVxilCo2mG1A-G-fm7buo-bJuKGC0"
TELEGRAM_CHAT_ID = "213349272"

# Fungsi helper
def get_telegram_url(endpoint="sendMessage"):
    """
    Generate URL for Telegram API
    """
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{endpoint}"

def create_send_message_payload(text, parse_mode="Markdown", disable_web_page_preview=False):
    """
    Create a payload for sending messages
    """
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode
    }
    
    if disable_web_page_preview:
        payload["disable_web_page_preview"] = True
        
    return payload