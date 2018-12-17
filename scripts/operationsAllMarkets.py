from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process(time, rdd):
    print("---- Top 10 markets by order type and number ---- %s ----" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(market=w[0][0], op=w[0][1], num_ops=w[1]))
        # create a DF from the Row RDD
        numOperations_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        numOperations_df.registerTempTable("markets_operations")
        # get the top 10 hashtags from the table using SQL and print them
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
    conf.setAppName('DataAnalyzer')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 20)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint('checkpoint_DataAnalyzer')
    dataStream = ssc.socketTextStream('localhost',9009)

    #operaciones sobre datos
    opNum = numberOfOperations(dataStream)
    opNumTotals = opNum.updateStateByKey(aggregate_count)
    opNumTotals.foreachRDD(process)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
