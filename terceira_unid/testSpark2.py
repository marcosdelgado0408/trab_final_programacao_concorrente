import findspark

findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange


class Point(object):
    def __init__(self, x, y, value, distance=None):
        self.x = x
        self.y = y
        self.value = value
        self.distance = 0






def printer(x):
    print(x)


def printPoints(x):
    print("x-> {}, y->{}, val->{}, dist -> {}".format(x.x, x.y, x.value, x.distance))



# 3 -> x / 4 -> y / 12 -> val
def criarPoints(x):
    splittedLinha = x.split(",")
    eachPoint = Point(splittedLinha[3], splittedLinha[4], splittedLinha[12])
    # print("x-> {}, y->{}, val->{}".format(eachPoint.x, eachPoint.y, eachPoint.value))

    return eachPoint








sc = SparkContext(appName="Knn")

kafkaParams = {"metadata.broker.list": "localhost:9092"}

start = 1 # pular primeira linha
until = 5
partition = 0
topic = 'csvtopic'
offset = OffsetRange(topic, partition, start, until)
offsets = [offset]


# Testing point
testPoint = Point(2, 7.45, 0)
k = 3




print(" >>>>>>>> CONSUMINDO KAFKA <<<<<<<<")

rdd = KafkaUtils.createRDD(sc, kafkaParams, offsets)

linhas = rdd.map(lambda x: x[1])


linhas.foreach(printer)

arr = linhas.map(criarPoints).foreach(printPoints)









# lista = rdd.collect()




print(" >>>>>>>> FIM DO CONSUMO <<<<<<<<")