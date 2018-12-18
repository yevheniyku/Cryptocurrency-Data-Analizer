from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

################################################################################
# Total Operations (Script 1)
#
# Calcula el total numero de operaciones de cada mercado, sacando los 10 más
# importantes. Lo cual permite saber donde se mueve mas dinero en un momento
# exacto
################################################################################

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process(time, rdd):
    print("---- Number of Operations ---- %s ----" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        # convertimos rdd en fila
        row_rdd = rdd.map(lambda w: Row(market=w[0], num_operations=w[1]))
        # creamos un data frame de la fila rdd
        numOperations_df = sql_context.createDataFrame(row_rdd)
        # registramos el data frame como tabla
        numOperations_df.registerTempTable("num_operations_market")
        # mostramos top 10 mercados por la cantidad de operaciones
        numOperations_counts_df = sql_context.sql("select market, num_operations from num_operations_market order by num_operations desc limit 10")
        numOperations_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def numberOfOperations(dataStream):
    data = dataStream.map(lambda data: data.split(','))
    markets = data.map(lambda market: market[0])
    marketsOps = markets.map(lambda op: (op, 1))
    marketsOps = marketsOps.reduceByKey(lambda x, y: x + y)

    return marketsOps

def main():
    conf = SparkConf()
    conf.setAppName('TotalOperations')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint('checkpoint_TotalOperations')
    dataStream = ssc.socketTextStream('localhost', 9009)

    #operaciones sobre datos
    opNum = numberOfOperations(dataStream)
    opNumTotals = opNum.updateStateByKey(aggregate_count)
    opNumTotals.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
