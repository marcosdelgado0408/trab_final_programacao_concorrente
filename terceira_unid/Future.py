from concurrent.futures import ThreadPoolExecutor
import threading
import random
import math



class Point(object):
    def __init__(self, x, y, value, distance = None):
        self.x = x
        self.y = y
        self.value = value



def distance(arr, p):
    for i in range(0, len(arr)):
        arr[i].distance = math.sqrt((arr[i].x - p.x) * (arr[i].x - p.x) + (arr[i].y - p.y) * (arr[i].y - p.y))

    return arr
    

def sort(arr):
    
    arr.sort(key = lambda x: x.distance)

    return arr
  

def media(arr, k):
    media = 0
    for i in range(k):
        media += arr[i].value
        
    print("The value classified to unknown point is: {}".format(media/k))


def classifyPoint(points, p, k):



    executor = ThreadPoolExecutor(max_workers = 4)


    future1 = executor.submit(distance, (points), (p))
    
    future2 = executor.submit(sort, (future1.result()))
    
    future3 = executor.submit(media, (future2.result()), (k))



    





def main():
  
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
  






  
if __name__ == '__main__':
    main()