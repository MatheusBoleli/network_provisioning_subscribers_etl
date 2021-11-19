#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests, os, urllib3, time,json, pandas as pd, csv,time, os, warnings, shutil, sys,pymysql, lxml, re, itertools, openpyxl, glob, mysql.connector, sys
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from requests.exceptions import ConnectionError
from control_methods import ControlMethods
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ProvisioningServices:
    
    def __init__(self, nomeAcao):
        
        #TEMPO
        self.tempo = datetime.now() - timedelta()
        self.dataBanco = self.tempo.strftime('%Y-%m-%d %H:%M:%S')
        self.dataLog = self.tempo.strftime('%Y-%m-%d_%H-%M-%S')
        self.padraoLog = '\n'+self.dataLog+'||class-provservices||R2D2-LOG||'+nomeAcao+'||'

        self.meth = ControlMethods(self.padraoLog)
        self.queryToken = '''SELECT id, token_api, generation_date FROM api_tokens WHERE date_format(generation_date, '%Y-%m-%d') >= curdate() - INTERVAL 19 HOUR;'''
        self.log = self.meth.log

    #=======================================================================================================================
    #=======================================================================================================================
    #=======================================================================================================================
    #AUTENTICAÇÃO
    
    #METODO PARA CHECAR TOKEN
    def check_auth(self, connAutomation):
        
        #BUSCANDO TOKEN NO BANCO DA AUTOMAÇÃO
        try:
            dfToken = pd.read_sql(self.queryToken, con=connAutomation).astype(str)
            cursor = connAutomation.cursor()
        except ConnectionError as e: 
            erro = 'Falha ao conectar ao banco da Automação'
            self.meth.write_log(erro)
                
        #VERIFICANDO SE HÁ TOKEN DO DIA, CASO CONTRARIO GERAR UM NOVO
        if dfToken.empty:
            
        
            #SETANDO: DATA ATUAL, URL DA API, HEADERS OBRIGATORIOS DA API
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            url = 'https://auth.api.com.br/tokens'
            headers = {'Authorization': 'Basic 11111111111111', 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}

            #DANDO UM POST PRA GERAR O TOKEN E CAPTURAR
            try:
                r = requests.post(url, headers=headers,verify=False,data='grant_type=example')
            except ConnectionError as e:    # This is the correct syntax
                    r = { "text": "Erro de conexão ao gerar o token", "status": 3333}
                    erro = 'Erro de conexão ao gerar o token||status:3333'
                    self.meth.write_log(erro)
                    return ''

            #CONVERTENDO RESPOSTA EM JSON
            json_object = json.loads(r.text)
            token = json_object["access_token"]
            
            print(token)
            
            comando = 'INSERT INTO api_tokens(token_api, generation_date) VALUES("'+token+'","'+self.dataBanco+'");'
            
            cursor.execute(comando)
            self.meth.write_log('Token não encontrado, Executando comando: INSERT INTO api_tokens(token_api, generation_date) VALUES("'+token+'","'+self.dataBanco+'");')
            connAutomation.commit()
            self.meth.write_log('New Token Generated!')

        else:
            self.meth.write_log('Utilizando ultimo token gerado.')
            token = dfToken['token_api'].values[0]
        
        return token
    
    #=======================================================================================================================
    #=======================================================================================================================
    #=======================================================================================================================
    #GETHLR
    
    def get_hlr(self, api_token, msisdn):
        
        baseUrl = 'https://auth.api.com.br'
        token = 'Bearer ' + api_token
        headers = {'Authorization': token, 'Content-Type': 'application/json'}
        url = baseUrl + "/hlr"
        urltail = ""
        
        if msisdn.startswith('00000') and len(msisdn) == 15:
            urltail = f'?imsi={ msisdn}'
            
        elif msisdn.startswith('55') and len(msisdn) == 13:
            urltail = f'?msisdn={ msisdn}'
            
        else:
            erro = 'Entrada inválida informada||status:404' 
            r = { "text": erro + '\n Entrada: '+ str(msisdn), "status": "404"}
            self.meth.write_log(erro)
            return r

        url = url+urltail
    
        try:
            r = requests.get(url, headers=headers,verify=False)
            r = { "text": json.loads(r.text), "status": r.status_code}
            
        except ConnectionError as e:   
                r = { "text": "Erro de conexão, tente novamente ou contate o suporte", "status": "404"}
                erro = 'Erro de conexão, tente novamente ou contate o suporte || status:404'
                self.meth.write_log(erro)
        return r


    #=======================================================================================================================
    #=======================================================================================================================
    #=======================================================================================================================
    #DELETE
    
    def delete(self, api_token, msisdn, imsi):

        #SETANDO VARIAVEIS PADRÃO
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        baseUrl = 'https://auth.api.com.br'
        token = 'Bearer ' + api_token
        headers = {'Authorization': token, 'Content-Type': 'application/json'}

        if imsi.startswith('00000') and len(imsi) == 15:
            urltailImsi = f'?imsi={ imsi}'
        else:
            erroimsi = 'Entrada de Imsi inválida informada!' 
            r = { "text": erroimsi + '\n Entrada: '+ str(imsi), "status": "404"}
            return r

        if msisdn.startswith('55') and len(msisdn) == 13:
            urltailMsisdn = f'?msisdn={ msisdn}'
        else:
            erromsisdn = 'Entrada de Msisdn inválida informada!' 
            r = { "text": erromsisdn + '\n Entrada: '+ str(msisdn), "status": "404"}
            return r

        #SETANDO URL HLR
        urlHlr = baseUrl + "/hlr"

        urlHlrMsisdn = urlHlr + urltailMsisdn
        urlHlrImsi = urlHlr + urltailImsi
        urlHlr = urlHlr+urltailMsisdn+'&'+urltailImsi

        #SETANDO URL HSS
        urlHss = baseUrl + "/hss/"
        urlHss = urlHss + imsi

        #SETANDO URL AUC
        urlAuc = baseUrl + "/auc/"
        urlAuc = urlAuc + imsi


        #DELETANDO HLR
        try:
            responseHlr1 = requests.delete(urlHlr, headers=headers,verify=False)
            responseHlr1 = { "text": json.loads(responseHlr1.text), "status": responseHlr1.status_code}
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão hlr ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão hlr, tente novamente ou contate o suporte", "status": "404"}
                return r


        try:
            responseHlr2 = requests.delete(urlHlrMsisdn, headers=headers,verify=False)
            responseHlr2 = { "text": json.loads(responseHlr2.text), "status": responseHlr2.status_code}
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão hlr ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão hlr, tente novamente ou contate o suporte", "status": "404"}
                return r

        try:
            responseHlr3 = requests.delete(urlHlrImsi, headers=headers,verify=False)
            responseHlr3 = { "text": json.loads(responseHlr3.text), "status": responseHlr3.status_code}
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão hlr ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão hlr, tente novamente ou contate o suporte", "status": "404"}
                return r

        #DELETANDO DO HSS
        try:
            responseHss = requests.delete(urlHss, headers=headers,verify=False)
            responseHss = { "text": json.loads(responseHss.text), "status": responseHss.status_code}
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão hss ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão hss, tente novamente ou contate o suporte", "status": "404"}
                return r


        #DELETANDO DO AUC
        try:
            responseAuc = requests.delete(urlAuc, headers=headers,verify=False)
            responseAuc = { "text": json.loads(responseAuc.text), "status": responseAuc.status_code}
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão auc ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão auc, tente novamente ou contate o suporte", "status": "404"}
                return r

        responseHlr1.update(responseHlr2)
        responseHlr1.update(responseHlr3)
        responseHlr1.update(responseHss)
        responseHlr1.update(responseAuc)

        return responseHlr1
    
    #=======================================================================================================================
    #=======================================================================================================================
    #=======================================================================================================================
    #CREATE
    
    def create(self, api_token, msisdn, imsi, ki, fsetind, adkey, a4ind, apnid):

        if imsi.startswith('00000') and len(imsi) == 15:
            imsiValidation = True
        else:
            erroimsi = 'Entrada de Imsi inválida informada!' 
            r = { "text": erroimsi + '\n Entrada: '+ str(imsi), "status": "404"}
            return r

        if msisdn.startswith('55') and len(msisdn) == 13:
            MsisdnValidation = True
        else:
            erromsisdn = 'Entrada de Msisdn inválida informada!' 
            r = { "text": erromsisdn + '\n Entrada: '+ str(msisdn), "status": "404"}
            return r

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        baseUrl = 'https://auth.api.com.br'
        api_token = 'Bearer ' + api_token
        headers = {'Authorization': api_token, 'Content-Type': 'application/json'}
        urlAuc = baseUrl + "/aucsub"
        urlHlr = baseUrl + "/hlr"
        urlHss = baseUrl + "/hss"


        jsonAuc = {'MOId' : {'imsi':imsi}, 'MOAttributes':{'ki':ki, 'adkey':adkey, 'fsetind':fsetind, 'a4ind':a4ind}}
        jsonHlr = {"MOId": {"msisdn": msisdn,"imsi": imsi},"MOAttributes": {"hlrSub": {"profileId": "1","csp": "100","gprs": {
                    "operation": "0","apnid": apnid,"eqosid": "417","vpaa": "0"},"nam": {"prov": "0"},"boic": {"provisionState": "1",
                    "ts10": {"activationState": "1"}},"cat": "224","clip": "1","mpty": "1"}}}
        jsonHss = {"MOId": {"msisdn": msisdn,"imsi": imsi},"MOAttributes": {"copyHlr": "1"}}

        jsonPatch1 = {"MOId": {"imsi": imsi},"MOAttributes": {"hssEps": {"epsIndividualContextId": [apnid, ",DEF"],"epsSessionTransferNumber": "99999"}}}
        jsonPatch2 = {"MOId": {"imsi": imsi},"MOAttributes": {"hssEps": {"epsIndividualDefaultContextId": apnid}}}
        jsonPatch3 = {"MOId": {"imsi": imsi},"MOAttributes": {"hssEps": {"epsIndividualContextId": ["260,DEL", "1,DEL"]}}}


        #REALIZANDO CREATE NO AUC
        try:
            createAuc = requests.post(urlAuc, headers=headers,verify=False, json=jsonAuc)
            aucResp = createAuc.status_code
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão no AUC ou ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão, tente novamente ou contate o suporte", "status": "404"}
                return r

        #REALIZANDO CREATE NO HLR
        try:
            createHlr = requests.post(urlHlr, headers=headers,verify=False, json=jsonHlr)
            hlrResp = createHlr.status_code
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão no HLR ou ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão, tente novamente ou contate o suporte", "status": "404"}
                return r

        #REALIZANDO CREATE NO HSS
        try:
            createHss = requests.post(urlHss, headers=headers,verify=False, json=jsonHss)
            hssResp = createHss.status_code
        except ConnectionError as e:   
                self.meth.write_log('\n||Erro de conexão no HSS ou ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão, tente novamente ou contate o suporte", "status": "404"}
                return r

        try:
            patch1 = requests.patch(urlHss, headers=headers,verify=False, json=jsonPatch1)
            patch2 = requests.patch(urlHss, headers=headers,verify=False, json=jsonPatch2)
            patch3 = requests.patch(urlHss, headers=headers,verify=False, json=jsonPatch3)
            patch1response = patch1.status_code
            patch2response = patch2.status_code
            patch3response = patch3.status_code

        except ConnectionError as e:   
                self.meth.write_log('\n||Erro ao realizar PATCHS ou ao gerar o token||' +now +'||'+ str(e))
                r = { "text": "Erro de conexão, tente novamente ou contate o suporte", "status": "404"}

        responseCreate = {'auc':aucResp, 'hlr':hlrResp, 'hss':hssResp, 'patch1':patch1response, 'patch2':patch2response,'patch3':patch3response,}

        return responseCreate
    
    
    #=======================================================================================================================
   