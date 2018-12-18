# Cryptocurrency Data Analyzer

Cryptocurrency Data Analyzer (CDA) es un sistema que analiza grandes volumenes de datos en tiempo real. Estos datos, provienen de la API pública de Bittrex, y para analizarlos se utilizan algoritmos y tecnologías de Big Data.

CDA consiste de dos partes:
- Algoritmo __data_retriever.py__: hace una petición a la API de Bittrex para obtener las últimas operaciones de cada mercado (ver el archivo __markets.txt__) en formato JSON. La respuesta se procesa y se manda al Spark utilizando un socket (localhost:9009)
- Algoritmos de Spark: reciben la información recibida por el socket y procesan los datos.   

Listado de scripts de spark:
- totalOperations.py: Calcula el total numero de operaciones de cada mercado, sacando los 10 más importantes.  
- operationTypes.py:  Calcula la tendencia de compra o venta del mercado global en tiempo real.
- operationsAllMarkets.py: Calcula el tipo de operaciones que predomina en cada mercado.
- operationsByMarket.py: Calcula las operaciones de un solo mercado y muestra el precio de la criptomoneda en cada operación. (Es necesario indicar el mercado que queremos analizar).
- mediumPrice.py: Precio medio de las criptomonedas.

## Requisitos
- Sistema Operativo basado en GNU/Linux
- Python 3
- Apache Spark


## Instalación
La guía de instalación está elaborada para Ubuntu
### Python && pip
```bash
sudo apt-get install python python3 python-pip
```

### Librería Requests
```bash
pip install requests
```

### Apache Spark
```bash
sudo apt­-add­-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
```
Descargar Apache Spark
```bash
sudo curl ­-O http://d3kbcqa49mib13.cloudfront.net/spark­-2.2.0­-bin­-hadoop2.7.tgz
sudo tar xvf ./spark­-2.2.0­-bin­-hadoop2.7.tgz
sudo mkdir /usr/local/spark
sudo cp -r spark­-2.2.0­-bin­-hadoop2.7/* usr/local/spark
```
Configuración de entorno: añadir '/usr/local/spark/bin' a PATH

## Uso

Abrir dos terminales. En una ejecutar:
```bash
python data_retriever.py
```

En otra terminal ejecutar:
```bash
spark-submit scripts/nombreDelScript
```

Para el script __operationsByMarket.py__ es necesario indicar el mercado que se va a analizar. Por ejemplo:

```bash
spark-submit scripts/operationByMarket.py USD-BTC
```

La salida con los resultados del análisis se verá por la consola de Apache Spark

## Contribución
Toda la contribución es bienvenida.


## Licencia
[GLP](https://choosealicense.com/licenses/gpl-3.0/)
