#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import telegram, requests
from datetime import datetime, timedelta


class TelegramServices:

    #TELEGRAM ACCESS
    my_token = 'token_telegram_bot'
    tokenGroup = 'token_telegram_group'
    
    #TELEGRAM METHODS
    def send(self,msg, chat_id=tokenGroup, token=my_token):
        """
        Send a message to a telegram user or group specified on chatId
        chat_id must be a number!
        """
        bot = telegram.Bot(token=token)
        bot.sendMessage(chat_id=chat_id, text=msg)

