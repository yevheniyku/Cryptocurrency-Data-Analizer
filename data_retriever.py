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
# Escribe una fila en el csv
################################################################################
def writeRow(writer, market, i, temp):
    row = [market, i['OrderType'], str(i['Price']), str(i['Quantity']), i['TimeStamp']]
    writer.writerow(row)

################################################################################
# Recorre json de respuesta y escribe las compras/ventas en el csv si
# la compra/venta es posterior a la ultima guardada
################################################################################
def loopsTrades(writer, market, data, file, timeStamp, count):
    global marketLastTrades

    for i in data['result']:
        if(i['TimeStamp'] > marketLastTrades[count+1][1]):
            writeRow(writer, market, i, file)

    marketLastTrades[count] = (market, timeStamp)

################################################################################
# Recorre json de respuesta y escribe las compras/ventas en el csv
################################################################################
def firstLoopTrades(writer, market, data, file, timeStamp):
    global marketLastTrades

    marketLastTrades.append((market, timeStamp))

    for i in data['result']:
        writeRow(writer, market, i, file)

################################################################################
# Obtiene el historial de las ordenesde compra/venta de las criptomonedas
################################################################################
def retrieveMarketHistory():
    global marketsList
    global marketLastTrades
    global firstLoop
    timeStamp = ''
    count = 0

    with open('marketsHistory.csv', 'a+') as file:
        writer = csv.writer(file)
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
                    firstLoopTrades(writer, market, data, file, timeStamp)
                else:
                    loopsTrades(writer, market, data, file, timeStamp, count)

                count += 1

        print('loop completed')
        firstLoop = False


################################################################################
# Guarda en una lista los mercados que se van a consultar
################################################################################
def retrieveMarkets():
    global marketsList
    # la peticion a la api
    with open('markets.txt', 'r') as f:
        for line in f:
            marketsList.append(line.rstrip('\n'))

################################################################################
# Funcion main
################################################################################
def main():
    retrieveMarkets()
    # para que el script de obtencion del historial funcione constantemente
    # he puesto un while(true) y para que no haya conflictos de datos
    # despues de cada ejecucion se para durante 1 minutos
    while(1):
        retrieveMarketHistory()
        time.sleep(60)


if __name__ == "__main__":
    main()
