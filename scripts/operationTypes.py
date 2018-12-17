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
    print("---- Operation Types ---- %s ----" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(operation_type=w[0], count_operation=w[1]))
        # create a DF from the Row RDD
        operationType_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        operationType_df.registerTempTable("operation_types")
        # get the top 10 hashtags from the table using SQL and print them
        operationType_df_counts_df = sql_context.sql("select operation_type, count_operation from operation_types order by count_operation desc limit 10")
        operationType_df_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def operationTypes(dataStream):
    data = dataStream.map(lambda data: data.split(','))
    types = data.map(lambda x: x[1])
    typesCount = types.map(lambda x: (x, 1))
    typesCount = typesCount.reduceByKey(lambda x,y: x+y)

    # enviar rdd
    return typesCount

def main():
    conf = SparkConf()
    conf.setAppName('DataAnalyzer')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 20)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint('checkpoint_DataAnalyzer')
    dataStream = ssc.socketTextStream('localhost',9009)

    opTypes = operationTypes(dataStream)
    opTypesTotals = opTypes.updateStateByKey(aggregate_count)
    opTypesTotals.foreachRDD(process)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()

if __name__ == '__main__':
    main()