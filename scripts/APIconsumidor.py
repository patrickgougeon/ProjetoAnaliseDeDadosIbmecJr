import requests
import schedule
import time
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Acessando os dados sensíveis
load_dotenv()

apiKeyAviacao = os.getenv("API_KEY_AVIACAO")
dbKey = os.getenv('DB_KEY')
dbHost = os.getenv('DB_HOST')

# --- Definindo info dos tres aeroportos do RJ ---

galeao = {
    'icao': 'SBGL',
    'latLong': '-22.805012635537793, -43.25663843421791',
}

santosDumont = {
    'icao': 'SBRJ',
    'latLong': '-22.90963775862103, -43.16379474770623',
}

jacarepagua = {
    'icao': 'SBJR',
    'latLong': '-22.987369680829218, -43.3700145323598',
}

aeroportos = [galeao, santosDumont, jacarepagua]

# --- Definindo as URLs e parâmetros das APIs ---

# Aviationstack API
urlAviacao = 'https://api.aviationstack.com/v1/flights'
paramsAviacao = {
    'access_key': apiKeyAviacao,
    'arr_icao': santosDumont['icao'],
    'offset': 0
}

# --- Funções ---

sqlInsertCompanhia = "INSERT IGNORE INTO companhia (icao, nome) VALUES (%s, %s);"
sqlInsertAeroporto = "INSERT IGNORE INTO aeroporto (icao, nome) VALUES (%s, %s);"
sqlInsertVoo = "INSERT IGNORE INTO voo (icao, aeroporto_icao, companhia_icao, h_chegada, data_chegada) VALUES (%s, %s, %s, %s, %s);"

def insertCompanhia(cursor, dados):
    companhiaAtual = []

    companhiaAtual.append(dados['airline']['icao'])
    companhiaAtual.append(dados['airline']['name'])

    cursor.execute(sqlInsertCompanhia, companhiaAtual)

def insertAeroporto(cursor, dados):
    aeroportoAtual = []

    aeroportoAtual.append(dados['arrival']['icao'])
    aeroportoAtual.append(dados['arrival']['airport'])

    cursor.execute(sqlInsertAeroporto, aeroportoAtual)


def insertVoo(cursor, dados):
    vooAtual = []

    vooAtual.append(dados['flight']['icao'])
    vooAtual.append(dados['arrival']['icao'])
    vooAtual.append(dados['airline']['icao'])
    vooAtual.append(dados['arrival']['scheduled'].split('T')[1])
    vooAtual.append(dados['flight_date'])

    cursor.execute(sqlInsertVoo, vooAtual)

# --- Inicializando o banco de dados ---

def main():
    try:
        conexao = mysql.connector.connect(
            host = dbHost,
            port = 3306,
            user = 'root',
            password = dbKey,
            database='aviacao_db'
        )

        if conexao.is_connected():

            print("Conexão com o banco de dados estabelecida")

            # --- Passando os dados da API para o banco ---

            cursor = conexao.cursor()
            
            for aeroporto in aeroportos:
                paramsAviacao['arr_icao'] = aeroporto['icao']
                paramsAviacao['offset'] = 0
                
                terminado = False
                contadorTotal = 0
                
                response = requests.get(urlAviacao, paramsAviacao)
                data = response.json()

                while not terminado :
                    for dados in data['data']:
                        insertCompanhia(cursor, dados)
                        insertAeroporto(cursor, dados)
                        insertVoo(cursor, dados)

                    contadorTotal += data['pagination']['count']

                    print(data['pagination'], paramsAviacao)
                    
                    if data['pagination']['total'] > contadorTotal:
                        paramsAviacao['offset'] += 100
                        response = requests.get(urlAviacao, paramsAviacao)
                        data = response.json()
                    else:
                        terminado = True
                    
            conexao.commit()

    except Error as e:
        print(f"Erro ao conectar com o banco de dados: {e}")

    finally:
        if 'conexao' in locals() and conexao.is_connected():
            conexao.close()
            print("Conexão com o banco de dados encerrada")

print('Rodando o programa!')
main()

# --- Definindo scheduler ---

schedule.every().day.at('00:00').do(main)
schedule.every().day.at('06:00').do(main)
schedule.every().day.at('12:00').do(main)
schedule.every().day.at('18:00').do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

