from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Step 1: Set up the Spark context

sc = SparkContext(appName="Socket")
ssc = StreamingContext(sc, batchDuration=3)  # 5 second batch interval

# Step 2: Connect to the TCP socket
host = "localhost"
port = 6100
def process_rdd(rdd):
    if not rdd.isEmpty():
        print("RDD contents:", rdd.collect())
    else:
        print("Empty RDD")

lines = ssc.socketTextStream("localhost", port)
lines.foreachRDD(process_rdd)

# Step 4: Start the streaming context and keep it alive
ssc.start()
ssc.awaitTermination()

