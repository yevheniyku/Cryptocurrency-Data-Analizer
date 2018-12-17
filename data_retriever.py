import json
import csv
import sys
import os
import time
import socket
#pip install requests
import requests

# Variables globales para crear la coneccion
TCP_IP = 'localhost'
TCP_PORT = 9009
conn = None

# Variables globales necesarias para la obtencion del historial
marketsList = []
marketHistoryURL = 'https://bittrex.com/api/v1.1/public/getmarkethistory?market='
marketLastTrades = [('', '')]
firstLoop = True

################################################################################
# Funcion que crea un socket por el cual se van a enviar los datos
################################################################################
def establishConnection():
    global TCP_IP
    global TCP_PORT
    global conn

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(0)
    conn, addr = s.accept()
    print('[INFO]Connected\n')

################################################################################
# Envia los datos al Spark por el socket
################################################################################
def sendDataToSpark(conn, market, i):
    try:
        dataPart1 = market + ',' + i['OrderType'] + ',' + str(i['Price'])
        dataPart2 = dataPart1 + ',' + str(i['Quantity']) + ','
        data = dataPart2 + i['TimeStamp']
        conn.send(data + '\n')
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

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
def loopsTrades(market, data, timeStamp, count):
    global marketLastTrades
    global conn

    for i in data['result']:
        if(i['TimeStamp'] > marketLastTrades[count+1][1]):
            sendDataToSpark(conn, market, i)

    marketLastTrades[count] = (market, timeStamp)

################################################################################
# Recorre json de respuesta y escribe las compras/ventas en el csv
################################################################################
def firstLoopTrades(market, data, timeStamp):
    global marketLastTrades
    global conn

    marketLastTrades.append((market, timeStamp))

    for i in data['result']:
        sendDataToSpark(conn, market, i)

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
                    firstLoopTrades(market, data, timeStamp)
                else:
                    loopsTrades(market, data, timeStamp, count)

                count += 1

        print('[INFO] Loop completed\n')
        firstLoop = False

################################################################################
# Guarda en una lista los mercados que se van a consultar
################################################################################
def retrieveMarkets():
    global marketsList
    # la peticion a la api
    with open('markets.txt', 'r') as f:
        for line in f:
            marketsList.append(line.rstrip('\n').rstrip('\r'))

################################################################################
# Funcion main
################################################################################
def main():
    establishConnection()
    retrieveMarkets()
    # para que el script de obtencion del historial funcione constantemente
    # he puesto un while(true) y para que no haya conflictos de datos
    # despues de cada ejecucion se para durante 1 minutos
    while(1):
        retrieveMarketHistory()
        time.sleep(20)

if __name__ == "__main__":
    main()
