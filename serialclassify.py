import random
import math



class Point(object):
    def __init__(self, x, y, value, distance = None):
        self.x = x
        self.y = y
        self.value = value



class ClassifyPoint(object):
    def __init__(self, arr, p, k):
        self.arr = arr
        self.p = p
        self.k = k


    def distance(self):
        for i in range(0, len(self.arr)):
            self.arr[i].distance = math.sqrt((self.arr[i].x - self.p.x) * (self.arr[i].x - self.p.x) + (self.arr[i].y - self.p.y) * (self.arr[i].y - self.p.y))

    

    def sort(self):
        
        self.arr.sort(key = lambda x: x.distance)

  

    def media(self):
        media = 0
        for i in range(self.k):
            media += self.arr[i].value
        
        print("The value classified to unknown point is: {}".format(media/self.k))


def classifyPoint(points, p, k):

    classifyPoint = ClassifyPoint(points, p, k)


    
    classifyPoint.distance()
    classifyPoint.sort()
    classifyPoint.media()





points = []

points.append(Point(1, 2, 4))
points.append(Point(3, 5, 2))
points.append(Point(1, 1, 9))
points.append(Point(4, 2, 8))


# testing point p(x,y)
p = Point(2.5, 7, 0)

# Number of neighbours 
k = 3

classifyPoint(points, p, k)
  




