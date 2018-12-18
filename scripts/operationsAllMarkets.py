from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

################################################################################
# Operations All Markets (Script 4)
#
# Calcula el tipo de operaciones que predominan en un mercado. Puede servir de
# aviso en caso de que muchos inversores empiecen a vender monedas
################################################################################

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process(time, rdd):
    print("---- Top 10 markets by order type and number ---- %s ----" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        # convertimos rdd en una fila
        row_rdd = rdd.map(lambda w: Row(market=w[0][0], op=w[0][1], num_ops=w[1]))
        # creamos un data frame
        numOperations_df = sql_context.createDataFrame(row_rdd)
        # registramos el data frame como una tabla
        numOperations_df.registerTempTable("markets_operations")
        # sacamos top 10 de mercados con las ordenes y numero de operaciones
        numOperations_counts_df = sql_context.sql("select market, op, num_ops from markets_operations order by num_ops desc limit 10")
        numOperations_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def numberOfOperations(dataStream):
    data = dataStream.map(lambda data: data.split(','))
    markets = data.map(lambda market: (market[0], market[1]))
    markets.groupByKey()
    marketsOps = markets.map(lambda op: (op, 1))
    marketsOps = marketsOps.reduceByKey(lambda x, y: x + y)
    return marketsOps

def main():
    conf = SparkConf()
    conf.setAppName('OperationsAllMarkets')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 20)
    # checkpoint para recuperar los RDD
    ssc.checkpoint('checkpoint_operationsAllMarkets')
    # utilizamos un socket para la entrada de datos
    dataStream = ssc.socketTextStream('localhost',9009)

    #operaciones sobre datos
    opNum = numberOfOperations(dataStream)
    opNumTotals = opNum.updateStateByKey(aggregate_count)
    opNumTotals.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
