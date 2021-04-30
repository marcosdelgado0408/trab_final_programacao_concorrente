import findspark
import math
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange
findspark.init()



class Point(object):
    def __init__(self, x, y, value, distance=None):
        self.x = x
        self.y = y
        self.value = value
        self.distance = 0



# Testing point
testPoint = Point(2, 7.45, 0)
k = 3







def printer(x):
    print(x)


def printPoints(x):
    print("x-> {}, y->{}, val->{}, dist -> {}".format(x.x, x.y, x.value, x.distance))


# [3] -> x / [4] -> y / [12] -> val
def criarPoints(x):
    splittedLinha = x.split(",")
    eachPoint = Point(splittedLinha[3], splittedLinha[4], splittedLinha[12])

    return eachPoint



def setDistance(eachpoint):
    # testPoint = global testPoint

    eachpoint.distance = math.sqrt( (float(eachpoint.x) - testPoint.x) * (float(eachpoint.x) - testPoint.x) + (float(eachpoint.y) - testPoint.y) * (float(eachpoint.y) - testPoint.y) )

    return eachpoint


# esse metodo transformara o array de objetos em um array so de valores -> apos a dist euclidiana e a ordenacao
def pegarVal(x):
    return float(x.value)



def media(lista):
    media = 0
    for i in lista:
        media += float(i)
    
    return media/len(lista)








if __name__ == "__main__":


    sc = SparkContext('local[*]', 'hands on PySpark')




    kafkaParams = {"metadata.broker.list": "localhost:9092"}

    start = 1 # pular primeira linha
    until = 500000
    partition = 0
    topic = 'csvtopic'
    offset1 = OffsetRange(topic, partition, start, until)
    # offset2 = OffsetRange('csvtopic', 0, 500001, 1000000)
    offsets = [offset1]






    print(" >>>>>>>> CONSUMINDO KAFKA <<<<<<<<")

    rdd = KafkaUtils.createRDD(sc, kafkaParams, offsets)


    linhas = rdd.map(lambda x: x[1])

    # linhas.foreach(printer)



    arr = linhas.map(criarPoints)\
        .map(setDistance)\
        .sortBy(lambda x: x.distance).map(pegarVal)




    # valores = arr.map(pegarVal)

 

 
    # pegando os k primeiros valores -> ja no formato de lista
    lista = arr.take(k)
    sc.stop()
    print(lista)
    print("The value classified to unknown point is: {}".format(media(lista)))




    print(" >>>>>>>> FIM DO CONSUMO <<<<<<<<")