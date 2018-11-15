import json
import csv
import sys
import os
import time
#pip install requests
import requests

marketsURL = 'https://bittrex.com/api/v1.1/public/getmarkets'
marketsList = []
marketHistoryURL = 'https://bittrex.com/api/v1.1/public/getmarkethistory?market='

################################################################################
# Obtiene el historial de las ordenesde compra/venta de las criptomonedas
# lo parsea y guarda en un archivo temporal
################################################################################
def retrieveMarketHistory():
    global marketsList

    with open('marketsHistory.temp', 'a+') as temp:
        # se hace una peticion a la API de Bittrex por cada marketname
        for item in marketsList:
            response = requests.get(marketHistoryURL + item)

            if(response.status_code != 200):
                print("Ha ocurrido un error a la hora de obtener el historial\n")
                sys.exit()
            else:
                data = json.loads(response.text)
                for i in data["result"]:
                    rowPart1 = item + '\t' + i['OrderType'] + '\t' + str(i['Price'])
                    rowPart2 = rowPart1 + '\t' + str(i['Quantity'])
                    row = rowPart2 +'\t '+ i['TimeStamp'] + '\n'
                    temp.write(row)

################################################################################
# Parsea el archivo temporal y crea una lista con los markets disponibles en
# Bittrex
################################################################################
def parseMarkets():
    with open('markets.temp', 'r') as temp:
        data = json.load(temp)

    global marketsList
    for item in data["result"]:
        marketsList.append(item['MarketName'])

    # borra el archivo temporal del sistema
    os.remove("markets.temp")

################################################################################
# Pide una lista de markets a la API de Bittrex y lo guarda en un archivo
# temporal
################################################################################
def retrieveMarkets():
    # la peticion a la api
    markets = requests.get(marketsURL)
    # si la respuesta es un error sale del programa
    if(markets.status_code != 200):
        print("Ha ocurrido un error a la hora de obtener los markets\n")
        sys.exit()
    else:
        # guarda los mercados disponibles en un archivo temporal
        with open('markets.temp', 'w') as temporal:
            data = json.loads(markets.text)
            print(json.dumps(data, indent=4, sort_keys=True), file=temporal)

def main():
    retrieveMarkets()
    parseMarkets()
    while(1):
        retrieveMarketHistory()
        time.sleep(120)


if __name__ == "__main__":
    main()
