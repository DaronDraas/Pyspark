import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\esteb\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\esteb\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder\
        .appName("hands-on-pyspark")\
        .config("spark.executor.memory","2g")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") #ALL, OFF, ERROR, DEBUG, INFO, WARN
spark.active()
        
from datetime import date
data = [
    {
        "nombre":"Marcelo",
        "nacimiento": date(1987,10,7),
        "mail":"marcelo@gmail.com",
        "x":5689,
        "y":15.26,
        "active":True
    },
    {
        "nombre":"Andrea",
        "nacimiento": date(1990,8,19),
        "mail":"andre@gmail.com",
        "x":4510,
        "y":14.058,
        "active":False
    },
    {
        "nombre":"Juan",
        "nacimiento": date(2000,9,7),
        "mail":"juan@gmail.com",
        "x":3000,
        "y":1.2,
        "active":True
    }
]

df1 = spark.createDataFrame(data)
df1.show()