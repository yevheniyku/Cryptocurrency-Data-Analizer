import json
import csv
import sys
import os
#pip install requests
import requests

marketsURL = 'https://bittrex.com/api/v1.1/public/getmarkets'
marketsList = []
marketHistoryURL = 'https://bittrex.com/api/v1.1/public/getmarkethistory?market='

################################################################################
#
################################################################################
def retrieveMarketHistory():
    global marketsList

    with open('marketsHistory.temp', 'a+') as temp:
        for item in marketsList:
            temp.write(item + '\n')
            response = requests.get(marketHistoryURL + item)

            if(response.status_code != 200):
                print("Ha ocurrido un error a la hora de obtener el historial\n")
                sys.exit()
            else:
                data = json.loads(response.text)
                print(json.dumps(data, indent=4, sort_keys=True), file=temp)

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
        with open('markets.temp', 'w') as temporal:
            data = json.loads(markets.text)
            print(json.dumps(data, indent=4, sort_keys=True), file=temporal)

def main():
    retrieveMarkets()
    parseMarkets()
    os.remove("markets.temp")
    retrieveMarketHistory()


if __name__ == "__main__":
    main()
