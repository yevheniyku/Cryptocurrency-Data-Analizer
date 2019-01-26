# Cryptocurrency Data Analyzer

<p align="center">
  <span>Language:</span> 
  <a href="https://github.com/yevheniyku/Cryptocurrency-Data-Analizer">Español</a> |
  <a href="https://github.com/yevheniyku/Cryptocurrency-Data-Analizer/blob/master/english.md">English</a> 
</p>

Cryptocurrency Data Analyzer (CDA) is a real-time system to analyze large data volumes. This data is provided by the public Bittrex API
and it is analyzed with Big Data algorithms and technologies.

CDA consists of two parts:
- Algorithm __data_retriever.py__: makes a request to the Bittrex API to obtain the latest operations of each market (see file __markets.txt__) in JSON format. The response is processed and sent to Spark using a socket (localhost: 9009)
- Spark Algorithms: receive the information of the socket and process the data.

Listing of spark scripts:
- totalOperations.py: Calculate the total number of operations of each market, taking out the 10 most important.
- operationTypes.py: Calculates the trend of buying or selling the global market in real time.
- operationsAllMarkets.py: Calculates the type of operations that predominates in each market.
- operationsByMarket.py: Calculates the operations of a single market and shows the price of the cryptocurrency in each operation. (It is necessary to indicate the market that we want to analyze).
- mediumPrice.py: Average price of cryptocurrencies.

## Requirements
- Operating System based on GNU / Linux
- Python 3
- Apache Spark

## Installation
The installation guide is written for Ubuntu

### Python && pip
```bash
sudo apt-get install python python3 python-pip
```

### Library Requests
```bash
pip install requests
```

### Apache Spark
```bash
sudo apt­-add­-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
```
Download Apache Spark
```bash
sudo curl ­-O http://d3kbcqa49mib13.cloudfront.net/spark­-2.2.0­-bin­-hadoop2.7.tgz
sudo tar xvf ./spark­-2.2.0­-bin­-hadoop2.7.tgz
sudo mkdir /usr/local/spark
sudo cp -r spark­-2.2.0­-bin­-hadoop2.7/* usr/local/spark
```
Environment configuration:add '/usr/local/spark/bin' to PATH

## Usage

Open two terminals. In one run:
```bash
python data_retriever.py
```
In another terminal execute:
```bash
spark-submit scripts/nombreDelScript
```

For the __operationsByMarket.py__ script it is necessary to indicate the market to be analyzed. For example:

```bash
spark-submit scripts/operationByMarket.py USD-BTC
```

The output with the results of the analysis will be seen by the Apache Spark console

## Contribution
All the contributions are welcome.


## License
[GLP](https://choosealicense.com/licenses/gpl-3.0/)
