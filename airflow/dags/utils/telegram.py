import os
import requests
import logging
from airflow.models import Variable

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_telegram_credentials():
    """
    Mendapatkan kredensial Telegram dari environment atau Airflow Variables
    """
    # Coba dari environment variables
    telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    # Jika tidak ada, coba dari Airflow Variables
    if not telegram_bot_token:
        try:
            telegram_bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
            logger.info("Successfully retrieved bot token from Airflow Variables")
        except:
            logger.warning("Failed to get TELEGRAM_BOT_TOKEN from Airflow Variables")
    
    if not telegram_chat_id:
        try:
            telegram_chat_id = Variable.get("TELEGRAM_CHAT_ID")
            logger.info("Successfully retrieved chat ID from Airflow Variables")
        except:
            logger.warning("Failed to get TELEGRAM_CHAT_ID from Airflow Variables")
            
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

def send_telegram_message(message, disable_web_page_preview=False):
    """
    Kirim pesan ke Telegram dengan error handling yang lebih baik
    """
    token, chat_id = get_telegram_credentials()
    if not token or not chat_id:
        logger.error("Missing Telegram credentials")
        return "Error: Missing Telegram credentials"
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    # Batasan panjang pesan Telegram 4096 karakter
    MAX_MESSAGE_LENGTH = 4000  # Sedikit margin untuk keamanan
    
    # Jika pesan lebih pendek dari batasan, kirim langsung
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
    # Jika pesan terlalu panjang, bagi menjadi beberapa bagian
    else:
        logger.info(f"Message too long ({len(message)} chars), splitting into parts")
        # Cari posisi baris baru untuk membagi pesan dengan rapi
        parts = []
        start_idx = 0
        
        while start_idx < len(message):
            # Jika sisa pesan masih muat dalam satu bagian
            if len(message) - start_idx <= MAX_MESSAGE_LENGTH:
                parts.append(message[start_idx:])
                break
            
            # Cari posisi baris baru terakhir yang masih dalam batas panjang
            cut_idx = start_idx + MAX_MESSAGE_LENGTH
            while cut_idx > start_idx:
                if message[cut_idx] == '\n':
                    break
                cut_idx -= 1
                
            # Jika tidak ada baris baru, potong di spasi
            if cut_idx == start_idx:
                cut_idx = start_idx + MAX_MESSAGE_LENGTH
                while cut_idx > start_idx:
                    if message[cut_idx] == ' ':
                        break
                    cut_idx -= 1
                    
            # Jika masih tidak ditemukan, potong tepat di MAX_MESSAGE_LENGTH
            if cut_idx == start_idx:
                cut_idx = start_idx + MAX_MESSAGE_LENGTH - 1
                
            # Tambahkan bagian ini ke daftar
            parts.append(message[start_idx:cut_idx+1])
            start_idx = cut_idx + 1
            
        # Kirim setiap bagian
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
                # Tunggu sebentar untuk menghindari rate limiting
                import time
                time.sleep(1)
            except Exception as e:
                logger.error(f"Exception sending part {i+1}: {str(e)}")
                
        if success_count == len(parts):
            return f"Message sent successfully in {len(parts)} parts"
        else:
            return f"Partial success: {success_count}/{len(parts)} parts sent"