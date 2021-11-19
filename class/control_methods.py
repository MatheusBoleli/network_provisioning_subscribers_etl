#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os, urllib3, time, pandas as pd, csv, warnings, shutil, sys, lxml, re, itertools, openpyxl, glob, sys
from datetime import datetime, timedelta
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ControlMethods:
     
    def __init__(self, padraoLog):
        
        self.padraoLog = padraoLog
        
        self.tempo = datetime.now() - timedelta()
        self.dataLog = self.tempo.strftime('%Y-%m-%d_%H-%M-%S')
        self.log = 'logs/detailed'
    

    #METODO PRA ESCRITURA DE LOGS
    def write_log(self,texto):
        
        texto = self.padraoLog + texto
        
        #SETANDO NOME COMPLETO DO ARQUIVO DE LOG
        detailed_log = datetime.now().strftime("-%Y-%m-%d-")+ 'log.txt'
        
        #UNINDO LOCAL + NOME DO ARQUIVO
        file = self.log +  detailed_log
        print(texto)
        
        #VERIFICANDO SE EXISTE UM LOG DO DIA, CASO NÃO EXISTA, CRIAR UM NOVO E ESCREVER O TEXTO PASSADO COMO PARAMETRO
        if os.path.exists(file):
            logs = open(file, 'a+')
            logs.write(texto)
            logs.close()
        else: 
            logs = open(file, 'w+')
            logs.write(texto)
            logs.close()
            
        return texto

    #CONVERSOR DE LISTA EM TUPLA
    def convert(self, list):
        return str(tuple(list)) 

