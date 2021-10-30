#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession, SQLContext, Row
from datetime import datetime, timedelta
from pyspark.sql.functions import *


# In[2]:


# Creo la sesion de spark
spark = SparkSession.builder.appName("testSession").getOrCreate()


# In[11]:

#Defino los dias que voy a correr el script
startDate = auxDate = datetime.strptime('2021-01-01', '%Y-%m-%d').date()
endDate= datetime.strptime('2021-01-05', '%Y-%m-%d').date()
idMax=0

#Tomo dia a dia para compararlo con dias anteriores y asi descartar registros duplicados
while auxDate <= endDate:
    year = auxDate.year
    month = auxDate.month
    day = auxDate.day
    #Datos para 1 dia antes
    year1 = (auxDate - timedelta(days=1)).year
    month1 = (auxDate - timedelta(days=1)).month
    day1 = (auxDate - timedelta(days=1)).day
    
    df = spark.read.parquet("gs://ds1-dataproc/archivos/out/devices.parquet/year={0}/month={1}/day={2}".format(year,month,day))
    
    try:
        df1 = spark.read.parquet("gs://ds1-dataproc/archivos/out/devices.parquet/year={0}/month={1}/day={2}".format(year1,month1,day1)) 
        df1.createOrReplaceTempView('df1')

        aux = spark.sql('SELECT MAX(id) as max from df1').collect()[0]

        idMax = int(aux['max'])
    except:        
        idMax=0
    df = df.filter(df['id'] > idMax)
    df.show(truncate=False)


    auxDate = auxDate + timedelta(days=1)
    
    #cargo en Big Query
    df.write   .format("bigquery")     .mode("append")   .option("temporaryGcsBucket","ds1-dataproc/temp")   .save("test-opi-330322.test.Base3") 
    

