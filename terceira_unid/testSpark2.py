import findspark

findspark.init()



from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange


sc = SparkContext(appName="Knn")

kafkaParams = {"metadata.broker.list": "localhost:9092"}

start = 0
until = 100
partition = 0
topic = 'csvtopic'
offset = OffsetRange(topic, partition, start, until)
offsets = [offset]


print(" >>>>>>>> CONSUMINDO KAFKA <<<<<<<<")

kafkaRdd = KafkaUtils.createRDD(sc, kafkaParams, offsets)

lista = kafkaRdd.collect()





print(" >>>>>>>> FIM DO CONSUMO <<<<<<<<")