#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests, os, urllib3, time,json, pandas as pd, csv, warnings, shutil, sys, lxml, re, itertools, openpyxl, glob, mysql.connector, sys
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from requests.exceptions import ConnectionError
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class mysqlConns:
    
    #METODO DE CONEXÕES AO BANCO
    def engine_create(self):
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                               .format(user="BOLELI",
                                       pw="1234",
                                       db="analises_prov"))
        return engine

    def open_connection_automation(self):
        conn = mysql.connector.connect(
        host="localhost",
        user="BOLELI",
        password="1234",
        database="analises_prov")
        return conn

    def open_connection_bss(self):
        conn = mysql.connector.connect(
        host="exampleurlbss.rds.amazonaws.com",
        user="usr",
        password="pass",
        database="database")
        return conn

    def close_connection(self ,x):
        x.close()

