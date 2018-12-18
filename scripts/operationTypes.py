from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

################################################################################
# Operation Type (Script 2)
#
# Calcula la tendencia de compra o venta en un momento exacto
################################################################################

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process(time, rdd):
    print("---- Operation Types ---- %s ----" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        # convertimos rdd en una fila
        row_rdd = rdd.map(lambda w: Row(operation_type=w[0], count_operation=w[1]))
        # creamos un data frame
        operationType_df = sql_context.createDataFrame(row_rdd)
        # registramos la tabla
        operationType_df.registerTempTable("operation_types")
        # sacamos el numero de
        operationType_df_counts_df = sql_context.sql("select operation_type, count_operation from operation_types order by count_operation desc")
        operationType_df_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def operationTypes(dataStream):
    data = dataStream.map(lambda data: data.split(','))
    types = data.map(lambda x: x[1])
    typesCount = types.map(lambda x: (x, 1))
    typesCount = typesCount.reduceByKey(lambda x,y: x+y)

    return typesCount

def main():
    conf = SparkConf()
    conf.setAppName('OperationsType')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint('checkpoint_DataAnalyzer')
    dataStream = ssc.socketTextStream('localhost',9009)

    opTypes = operationTypes(dataStream)
    opTypesTotals = opTypes.updateStateByKey(aggregate_count)
    opTypesTotals.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
