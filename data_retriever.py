import json
import csv
import sys
import os
import time
#pip install requests
import requests

marketsList = []
marketHistoryURL = 'https://bittrex.com/api/v1.1/public/getmarkethistory?market='
marketLastTrades = [('', '')]
firstLoop = True

################################################################################
#
################################################################################
def writeRow(market, i, temp):
    rowPart1 = market + '\t' + i['OrderType'] + '\t' + str(i['Price'])
    rowPart2 = rowPart1 + '\t' + str(i['Quantity'])
    row = rowPart2 +'\t '+ i['TimeStamp'] + '\n'
    temp.write(row)

################################################################################
#
################################################################################
def loopsTrades(market, data, file, timeStamp, count):
    global marketLastTrades

    for i in data['result']:
        if(i['TimeStamp'] > marketLastTrades[count+1][1]):
            writeRow(market, i, file)

    marketLastTrades[count] = (market, timeStamp)

################################################################################
#
################################################################################
def firstLoopTrades(market, data, file, timeStamp):
    global marketLastTrades

    marketLastTrades.append((market, timeStamp))

    for i in data['result']:
        writeRow(market, i, file)

################################################################################
# Obtiene el historial de las ordenesde compra/venta de las criptomonedas
# lo parsea y guarda en un archivo temporal.
# Para prevenir la repeticion de las ventas (existen markets muy poco activos)
# apuntamos el timestamp de la ultima venta de cada mercado en una lista
################################################################################
def retrieveMarketHistory():
    global marketsList
    global marketLastTrades
    global firstLoop
    timeStamp = ''
    count = 0

    with open('marketsHistory.csv', 'a+') as file:
        # se hace una peticion a la API de Bittrex por cada marketname
        for market in marketsList:
            response = requests.get(marketHistoryURL + market)
            #verificamos que la peticion ha tenido exito
            if(response.status_code != 200):
                print('Ha ocurrido un error a la hora de obtener el historial\n')
                sys.exit()
            else:
                data = json.loads(response.text)
                #registramos la ultima compra del market
                timeStamp = data['result'][0]['TimeStamp']

                if(firstLoop == True):
                    firstLoopTrades(market, data, file, timeStamp)
                else:
                    loopsTrades(market, data, file, timeStamp, count)

                count += 1

        print(marketLastTrades)
        firstLoop = False


################################################################################
#
################################################################################
def retrieveMarkets():
    global marketsList
    # la peticion a la api
    with open('markets.txt', 'r') as f:
        for line in f:
            marketsList.append(line.rstrip('\n'))

################################################################################
#
################################################################################
def main():
    retrieveMarkets()
    # para que el script de obtencion del historial funcione constantemente
    # he puesto un while(true) y pa que no haya conflictos de datos
    # despues de cada ejecucion se para durante 2 minutos
    while(1):
        retrieveMarketHistory()
        time.sleep(10)


if __name__ == "__main__":
    main()
