from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

################################################################################
# Operations By Market (Script 3)
#
# En caso de que un inversor quiera vigilar solo una moneda, este script puede
# servir ya que a la hora de ejecutar el script hay que indicar el mercado
# Ejemplo: USD-BTC
################################################################################

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process(time, rdd):
    print("---- Market operations ---- %s ----" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        # convertimos el RDD en una fila
        row_rdd = rdd.map(lambda w: Row(market=w[0][0], op=w[0][1], price=w[0][2], num_ops=w[1]))
        # creamos un data frame de una fila
        numOperations_df = sql_context.createDataFrame(row_rdd)
        # registramos la tabla del data frame
        numOperations_df.registerTempTable("ops_by_market")
        # mostramos las ultimas 5 ordenes de compra y venta
        buy_df = sql_context.sql("""select market, op, price, num_ops from ops_by_market
                                        where op='BUY' order by num_ops desc limit 5""")
        sell_df = sql_context.sql("""select market, op, price, num_ops from ops_by_market
                                        where op='SELL' order by num_ops desc limit 5""")
        buy_df.show()
        sell_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def numberOfOperations(dataStream, market):
    data = dataStream.map(lambda data: data.split(','))
    data = data.filter(lambda m: m[0] == market)
    markets = data.map(lambda market: (market[0], market[1], market[2]))
    marketsOps = markets.map(lambda op: (op, 1))
    marketsOps = marketsOps.reduceByKey(lambda x, y: x + y)
    return marketsOps

def main():
    conf = SparkConf()
    conf.setAppName('DataAnalyzer')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint('checkpoint_opsByMarket')
    dataStream = ssc.socketTextStream('localhost',9009)

    market = sys.argv[1]

    #operaciones sobre datos
    opNum = numberOfOperations(dataStream, market)
    opNumTotals = opNum.updateStateByKey(aggregate_count)
    opNumTotals.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
