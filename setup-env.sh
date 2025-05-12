#!/bin/bash

# Exit script if any command fails
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up environment for Airflow...${NC}"

# Check if .env file exists
if [ -f .env ]; then
    echo -e "${YELLOW}Warning: .env file already exists. Backing it up to .env.bak${NC}"
    cp .env .env.bak
fi

# Check if template exists
if [ ! -f .env.template ]; then
    echo -e "${RED}Error: .env.template file not found${NC}"
    exit 1
fi

# Copy template to .env
cp .env.template .env

# Generate Fernet key
echo -e "${GREEN}Generating Fernet key...${NC}"
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
sed -i "s|AIRFLOW__CORE__FERNET_KEY=|AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}|g" .env

# Use same key for webserver secret
sed -i "s|AIRFLOW__WEBSERVER__SECRET_KEY=|AIRFLOW__WEBSERVER__SECRET_KEY=${FERNET_KEY}|g" .env

# Prompt for Telegram bot token
echo -e "${YELLOW}Enter your Telegram Bot Token:${NC}"
read TELEGRAM_BOT_TOKEN
sed -i "s|TELEGRAM_BOT_TOKEN=|TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}|g" .env

# Prompt for Telegram chat ID
echo -e "${YELLOW}Enter your Telegram Chat ID:${NC}"
read TELEGRAM_CHAT_ID
sed -i "s|TELEGRAM_CHAT_ID=|TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}|g" .env

# Generate Telegram connection string
TELEGRAM_URL="http://https%3A%2F%2Fapi.telegram.org?base_url=https%3A%2F%2Fapi.telegram.org%2Fbot${TELEGRAM_BOT_TOKEN}%2F"
sed -i "s|AIRFLOW_CONN_TELEGRAM_CONN=|AIRFLOW_CONN_TELEGRAM_CONN=${TELEGRAM_URL}|g" .env

# Optional: Prompt for custom admin credentials
echo -e "${YELLOW}Do you want to set custom admin credentials? (y/n)${NC}"
read CUSTOM_ADMIN
if [[ $CUSTOM_ADMIN == "y" || $CUSTOM_ADMIN == "Y" ]]; then
    echo -e "${YELLOW}Enter Airflow admin username:${NC}"
    read ADMIN_USER
    sed -i "s|AIRFLOW_ADMIN_USER=admin|AIRFLOW_ADMIN_USER=${ADMIN_USER}|g" .env
    
    echo -e "${YELLOW}Enter Airflow admin password:${NC}"
    read ADMIN_PASSWORD
    sed -i "s|AIRFLOW_ADMIN_PASSWORD=admin|AIRFLOW_ADMIN_PASSWORD=${ADMIN_PASSWORD}|g" .env
fi

echo -e "${GREEN}Environment setup complete!${NC}"
echo -e "${GREEN}Your credentials are now stored in the .env file.${NC}"
echo -e "${YELLOW}Warning: Keep your .env file secure and do not commit it to version control.${NC}"