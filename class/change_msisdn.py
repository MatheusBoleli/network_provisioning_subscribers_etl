#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests, os, time, pandas as pd, csv, warnings, shutil, sys, lxml, re, itertools, openpyxl, glob
from datetime import datetime, timedelta
from mysql_services import mysqlConns
from telegram_services import TelegramServices
from api_provisioning import ProvisioningServices
from control_methods import ControlMethods
warnings.filterwarnings("ignore")

#===============================================================================================================

#DECLARANDO VARIAVEIS DE TEMPO/LOCAL/NOMES

#TEMPO
tempo = datetime.now() - timedelta()
hoje = tempo.strftime('%Y-%m-%d')
dataBanco = tempo.strftime('%Y-%m-%d %H:%M:%S')
dataLog = tempo.strftime('%Y-%m-%d_%H-%M-%S')
timeReport = tempo.strftime('%d/%m/%Y - %H:%M')

#NOMES
nomeAcao = 'change MSISDN'
padraoLog = '\n'+dataLog+'||class-main_change_msisdn||R2D2-LOG||change_Msisdn||'

#===============================================================================================================

#DEFININDO TELEGRAM MESSAGES

messageFalhaBss = '''REPORT AUTOMAÇÃO! \n
    Falha ao acessar o BSS! \n
    ''' + nomeAcao +''' \n '''+ timeReport

messageFalhaAutomacao = '''REPORT AUTOMAÇÃO! \n
    Falha ao acessar o Banco da Automacao! \n
    ''' + nomeAcao +''' \n '''+ timeReport

messageFalhaApi = '''REPORT AUTOMAÇÃO! \n
    Erro de API, verifique a conexão \n
    ''' + nomeAcao +''' \n '''+ timeReport

messageFalhaAnalise = '''REPORT AUTOMAÇÃO! \n
    Falha ao armazenar analise no banco \n
    ''' + nomeAcao +''' \n '''+ timeReport

temArquivo = False
feitoDownload = False

#===============================================================================================================

#Querys
queryBss = '''SELECT * FROM tb_.......'''

queryAutomacao= '''SELECT * FROM output WHERE....'''

queryBlackList = '''select msisdn FROM blacklist;'''

#===============================================================================================================
#===============================================================================================================

#INSTANCIANDO CLASSES A UTILIZAR
tg = TelegramServices()
sql = mysqlConns()
apiprov = ProvisioningServices(nomeAcao)
meth = ControlMethods(padraoLog)

#===============================================================================================================
#ABRINDO CONEXÕES COM BANCO DE DADOS
try:
    connAutomacao = sql.open_connection_automation()
    connBss = sql.open_connection_bss()
    engineAutomacao = sql.engine_create()
    meth.write_log(padraoLog + 'my_sql.initilized')

except:
    meth.write_log(padraoLog + 'mysql.initialized||connection_failed')
    sys.exit()
#===============================================================================================================

#BUSCANDO SCRIPTS DO DIA NO BSS E SAIDAS DO DIA E BLACKLIST NO BANCO DA AUTOMAÇÃO

try:
    dfBss = pd.read_sql(queryBss, con=connBss).astype(str)
    print('Logando no BSS, extraindo Query!')
    
except:
    erro = 'Falha ao Acessar o BSS! Encerrando o Script!'
    meth.write_log(padraoLog + erro)
    tg.send(messageFalhaBss)
    sys.exit()
#-----------------------------------------------------------------------------------------------

try:
    dfSaidas = pd.read_sql(queryAutomacao, con=connAutomacao).astype(str)
    dfBlackList = pd.read_sql(queryBlackList, con=connAutomacao).astype(str)
    print('Logando no BANCO DA AUTOMAÇÃO, extraindo Query!')
    
    if dfSaidas.empty:
        temArquivo = False
    else:
        temArquivo = True
    
except:
    erro = 'Falha ao Acessar o BANCO DA AUTOMAÇÃO! Encerrando o Script!'
    tg.send(messageFalhaAutomacao)
    meth.write_log(padraoLog + erro)
    sys.exit()

#TESTANDO CONEXÃO COM API
try:
    #BUSCANDO TOKEN
    token = apiprov.check_auth(connAutomacao)
    print('Autenticação na API realizada com sucesso!!!')
except:
    erro = 'Erro de autenticação na API!!'
    meth.write_log(padraoLog + erro)
    the_type, the_value, the_traceback = sys.exc_info()
    erro = 'API_connection_Failed' + '||' + str(the_type) + '||' + str(the_value) + '||' + str(the_traceback) + '||'
    meth.write_log(padraoLog + erro)
    tg.send(messageFalhaApi)
    sys.exit()

#-----------------------------------------------------------------------------------------------

#Realizando batimento com as saidas
if(temArquivo == True):
    try:
        
        #FAZENDO BATIMENTO DO BSS COM BANCO DA AUTOMAÇÃO E BLACKLIST E GERANDO NOVA SAIDA
        dfBss[['ACAO']] = 'CHANGE'
        dfBss = dfBss[~dfBss['MSISDN'].isin(dfSaidas['nuMsisdn'])].reset_index(drop=True)
        dfBss = dfBss[~dfBss['MSISDN'].isin(dfBlackList['nuMsisdn'])].reset_index(drop=True)
        dfSaidaGerar = dfBss.copy()
        dfSaidaGerar = dfSaidaGerar[['ACAO', 'MSISDN', 'IMSI']]
        
        if dfSaidaGerar.empty:
            meth.write_log(padraoLog + 'Saida em Branco, não foram gerados Scripts, encerrando Script')
            sys.exit()
        else:
            print('FOI GERADA SAIDA DO SCRIPT DE ALTERAÇÃO DE MSISDN\n')

            #INSERINDO NOVA SAIDA NO BANCO DA AUTOMAÇÃO
            dfBss = dfBss.rename(columns={"MSISDN":"nuMsisdn", "IMSI" : "nuImsi"})
            dfBss[['id_acao']] = 2
            dfBss[['dtSaida']] = dataBanco
            dfBss = dfBss[['nuMsisdn', 'nuImsi', 'id_acao', 'dtSaida', 'dtCriacaoBss']]
            dfBss.to_sql('r2d2_output',con = engineAutomacao, if_exists='append', index=False)
            print('FOI INSERIDA NOVA SAIDA NO BANCO DA AUTOMAÇÃO!!!')
    
    except:
        erro = 'Falha ao realizar batimento e gerar nova saida!!'
        meth.write_log(padraoLog + erro)
        sys.exit()

else:
    
    #SE NÃO HOUVER SAIDAS PARA BATER, GERAR UMA NOVA
    dfBss[['ACAO']] = 'CHANGE'
    dfSaidaGerar = dfBss.copy()
    dfSaidaGerar = dfSaidaGerar[['ACAO', 'MSISDN', 'IMSI' ]]
    print('NENHUM ARQUIVO ENCONTRANDO! GERANDO NOVA SAIDA!')
    
    #Inserindo saida no Banco da automação
    dfBss = dfBss.rename(columns={"MSISDN":"nuMsisdn", "IMSI" : "nuImsi"})
    dfBss[['id_acao']] = 2
    dfBss[['dtSaida']] = dataBanco
    dfBss = dfBss[['nuMsisdn', 'nuImsi',  'id_acao', 'dtSaida',  'dtCriacaoBss']]

    dfBss.to_sql('r2d2_output',con = engineAutomacao, if_exists='append', index=False)



#DELETANDO E CRIANDO TODOS OS NUMEROS GERADOS.

for row in dfSaidaGerar.itertuples():
    
    msisdn = str(row.MSISDN)
    imsi = str(row.IMSI)
    
    #DELETANDO OS NUMEROS PARA CRIAR
    try:
        try:
            responseDelete = apiprov.delete(token, msisdn, imsi)
            msisdnString = str(row.MSISDN)
            print('DELETANDO O MSISDN: ' + msisdnString)
        except:
            responseDelete = apiprov.delete(token, msisdn, imsi)
            msisdnString = str(row.MSISDN)
            print('TENTANDO DELETAR NOVAMENTE O MSISDN: ' + msisdnString)            
    except:
        msisdnString = str(row.MSISDN)
        erro = 'Falha ao realizar delete do MSISDN: ' + msisdnString
        meth.write_log(padraoLog + erro)

for row in dfSaidaGerar.itertuples():
    
    msisdn = str(row.MSISDN)
    imsi = str(row.IMSI)

    #CRIANDO OS NUMEROS
    try:
        try:
            responseCreate = apiprov.create(token, msisdn, imsi)
            print('CRIANDO O MSISDN: ' + msisdn)
            print(responseCreate)
        except:
            responseCreate = apiprov.create(token, msisdn, imsi)
            print('TENTANDO CRIAR NOVAMENTE O MSISDN: ' + msisdn)
            print(responseCreate)            
    except:
        erro = 'Falha ao realizar Create do MSISDN: ' + msisdn
        meth.write_log(padraoLog + erro)

#DANDO GET EM TODOS OS MSISDNS DA SAIDA GERADA
column_names = []

dfGet = pd.DataFrame(columns = column_names)

count = 0

try:
    for i in dfBss.itertuples():

        print('Dando Get em: ' + str(i.nuMsisdn) + ' Contagem em:' + str(count))
        count = int(count) + 1

        response = apiprov.get_hlr(token, i.nuMsisdn)
        status = response['status']

        if status == 200:
            imsi = response['text']['MOAttributes']['imsi']

            if i.nuImsi == imsi:

                details = {'nuMsisdn' : [i.nuMsisdn],
                           'nuImsi' : [i.nuImsi],
                           'id_acao' : ['2'],
                           'id_status' : ['0'],
                           'dtCriacao': dataBanco,
                           'dtCorrecao': dataBanco}

                df = pd.DataFrame(details)
                dfGet = dfGet.append(df)


            else:
                details = {'nuMsisdn' : [i.nuMsisdn],
                           'nuImsi' : [i.nuImsi],
                           'id_acao' : ['2'],
                           'id_status' : ['1'],
                           'dtCriacao': dataBanco,
                           'dtCorrecao': None}

                df = pd.DataFrame(details)
                dfGet = dfGet.append(df)
                msisdn = i.nuMsisdn
                msisdn = str(msisdn)
                erro = 'Msisdn falhou, imsi é divergente:'+msisdn
                meth.write_log(padraoLog + erro)


        else:
            details = {'nuMsisdn' : [i.nuMsisdn],
                        'nuImsi' : [i.nuImsi],
                        'id_acao' : ['2'],
                        'id_status' : ['1'],
                        'dtCriacao': dataBanco,
                        'dtCorrecao': None}

            df = pd.DataFrame(details)
            msisdn = i.nuMsisdn
            msisdn = str(msisdn)
            status = str(status)
            erro = 'Msisdn failed, status is:'+status+'||'+msisdn
            meth.write_log(padraoLog + erro)
            dfGet = dfGet.append(df)
except:
    the_type, the_value, the_traceback = sys.exc_info()
    erro = 'API_connection_Failed' + '||' + str(the_type) + '||' + str(the_value) + '||' + str(the_traceback) + '||'
    meth.write_log(padraoLog + erro)
#     tg.send(messageFalhaApi)

#INSERINDO SAIDAS DOS QUE NÃO ESTÃO NA ANALYTICS

dfGet = dfGet[['nuMsisdn', 'nuImsi', 'id_acao', 'id_status', 'dtCriacao', 'dtCorrecao']]

try:
    dfGet.to_sql('analytics',con = engineAutomacao, if_exists='append', index=False)
    sucesso = 'success to analize'
    meth.write_log(padraoLog + sucesso)

except:
    erro = 'Falha ao Armazenar Analise no Banco'
    meth.write_log(padraoLog + erro)
    tg.send(messageFalhaAnalise)
    sys.exit()

meth.write_log(padraoLog + 'Encerrando um Script')

