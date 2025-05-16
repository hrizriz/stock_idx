import os
import requests
import logging
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_telegram_credentials():
    """
    Get Telegram credentials from environment variables or Airflow Variables
    """
    # Try from environment variables first
    telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    # If not found, try from Airflow Variables
    if not telegram_bot_token:
        try:
            telegram_bot_token = Variable.get("telegram_bot_token")
            logger.info("Successfully retrieved bot token from Airflow Variables")
        except:
            try:
                # Try alternative variable name
                telegram_bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
                logger.info("Successfully retrieved bot token from Airflow Variables (uppercase)")
            except:
                logger.warning("Failed to get telegram_bot_token from Airflow Variables")
    
    if not telegram_chat_id:
        try:
            telegram_chat_id = Variable.get("telegram_chat_id")
            logger.info("Successfully retrieved chat ID from Airflow Variables")
        except:
            try:
                # Try alternative variable name
                telegram_chat_id = Variable.get("TELEGRAM_CHAT_ID")
                logger.info("Successfully retrieved chat ID from Airflow Variables (uppercase)")
            except:
                logger.warning("Failed to get telegram_chat_id from Airflow Variables")
            
    if telegram_bot_token:
        masked_token = f"{telegram_bot_token[:5]}...{telegram_bot_token[-5:]}"
        logger.info(f"Using token: {masked_token}")
    else:
        logger.error("Telegram bot token not found!")
        
    if telegram_chat_id:
        logger.info(f"Using chat ID: {telegram_chat_id}")
    else:
        logger.error("Telegram chat ID not found!")
        
    return telegram_bot_token, telegram_chat_id

def send_telegram_message(message, chat_id=None, token=None, disable_web_page_preview=False):
    """
    Send message to Telegram with better error handling
    
    Parameters:
    message (str): Message to send
    chat_id (str, optional): Telegram chat ID, defaults to None (uses credentials from get_telegram_credentials)
    token (str, optional): Telegram bot token, defaults to None (uses credentials from get_telegram_credentials)
    disable_web_page_preview (bool, optional): Whether to disable web page preview, defaults to False
    
    Returns:
    str: Response message
    """
    # Get credentials if not provided
    if not token or not chat_id:
        token_from_env, chat_id_from_env = get_telegram_credentials()
        
        # Use provided values or fallback to environment values
        token = token or token_from_env
        chat_id = chat_id or chat_id_from_env
    
    if not token or not chat_id:
        logger.error("Missing Telegram credentials")
        return "Error: Missing Telegram credentials"
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    # Telegram message length limit is 4096 characters
    MAX_MESSAGE_LENGTH = 4000  # Small margin for safety
    
    # If message is shorter than the limit, send directly
    if len(message) <= MAX_MESSAGE_LENGTH:
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": disable_web_page_preview
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            logger.info(f"Telegram response: {response.status_code}")
            
            if response.status_code == 200:
                return "Message sent successfully"
            else:
                return f"Error sending to Telegram: {response.status_code}, {response.text}"
        except Exception as e:
            logger.error(f"Exception sending to Telegram: {str(e)}")
            return f"Error exception: {str(e)}"
    # If message is too long, split into multiple parts
    else:
        logger.info(f"Message too long ({len(message)} chars), splitting into parts")
        # Find newline positions to split message cleanly
        parts = []
        start_idx = 0
        
        while start_idx < len(message):
            # If remaining message fits in one part
            if len(message) - start_idx <= MAX_MESSAGE_LENGTH:
                parts.append(message[start_idx:])
                break
            
            # Find the last newline within the length limit
            cut_idx = start_idx + MAX_MESSAGE_LENGTH
            while cut_idx > start_idx:
                if cut_idx < len(message) and message[cut_idx] == '\n':
                    break
                cut_idx -= 1
                
            # If no newline found, try to cut at a space
            if cut_idx == start_idx:
                cut_idx = start_idx + MAX_MESSAGE_LENGTH
                while cut_idx > start_idx:
                    if cut_idx < len(message) and message[cut_idx] == ' ':
                        break
                    cut_idx -= 1
                    
            # If still not found, just cut at MAX_MESSAGE_LENGTH
            if cut_idx == start_idx:
                cut_idx = start_idx + MAX_MESSAGE_LENGTH - 1
                
            # Add this part to the list
            parts.append(message[start_idx:cut_idx+1])
            start_idx = cut_idx + 1
            
        # Send each part
        success_count = 0
        for i, part in enumerate(parts):
            logger.info(f"Sending part {i+1}/{len(parts)}, length: {len(part)} chars")
            payload = {
                "chat_id": chat_id,
                "text": part,
                "parse_mode": "Markdown",
                "disable_web_page_preview": disable_web_page_preview
            }
            
            try:
                response = requests.post(url, json=payload, timeout=10)
                if response.status_code == 200:
                    success_count += 1
                else:
                    logger.error(f"Error sending part {i+1}: {response.status_code}, {response.text}")
                # Wait a moment to avoid rate limiting
                import time
                time.sleep(1)
            except Exception as e:
                logger.error(f"Exception sending part {i+1}: {str(e)}")
                
        if success_count == len(parts):
            return f"Message sent successfully in {len(parts)} parts"
        else:
            return f"Partial success: {success_count}/{len(parts)} parts sent"