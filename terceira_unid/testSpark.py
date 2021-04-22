import pyspark
import findspark

findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils



sc = SparkContext(appName="Knn")

ssc = StreamingContext(sc, 2)


# message -> tupla, onde sua segunda pos. sera o valor de verdade
message = KafkaUtils.createDirectStream(ssc, topics=['csvtopic'], kafkaParams={"metadata.broker.list": "localhost:9092"} )

print(message)

line = message.map(lambda x: x[1])

splitedline = line.flatMap(lambda x: x.split(","))




# words = message.map(lambda x: x[1]).flatMap(lambda x: x.split(" "))

# wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)


# wordcount.pprint()







ssc.start()
ssc.awaitTermination()


