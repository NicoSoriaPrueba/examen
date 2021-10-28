#!/usr/bin/env python
# coding: utf-8

# In[34]:


from pyspark.sql import SparkSession, SQLContext, Row
from datetime import datetime, timedelta
from pyspark.sql.functions import *


# In[35]:


def calcMaximo(df):
    df.createOrReplaceTempView('df')
    aux = spark.sql('SELECT MAX(id) as max from df').collect()[0]
    return aux['max']
    


# In[38]:


# Create a SparkSession under the name "reddit". Viewable via the Spark UI
spark = SparkSession.builder.appName("testSession").getOrCreate()


# In[40]:


startDate = auxDate = datetime.strptime('2021-01-01', '%Y-%m-%d').date()
endDate= datetime.strptime('2021-01-05', '%Y-%m-%d').date()
idMax=0
while auxDate <= endDate:
    year = auxDate.year
    month = auxDate.month
    day = auxDate.day
    #Datos para 3 dias antes
    year1 = (auxDate - timedelta(days=1)).year
    month1 = (auxDate - timedelta(days=1)).month
    day1 = (auxDate - timedelta(days=1)).day
    
    df = spark.read.parquet("gs://ds1-dataproc/archivos/out/devices.parquet/year={0}/month={1}/day={2}".format(year,month,day))
    try:
        df1 = spark.read.parquet("gs://ds1-dataproc/archivos/out/devices.parquet/year={0}/month={1}/day={2}".format(year1,month1,day1)) 
        idMax = calcMaximo(df1)
    except:
        idMax=0
    df = df.filter(df.id > idMax)
    print(df.show(truncate=False))
    df.write.format('bigquery')     .option("temporaryGcsBucket","gs://ds1-dataproc/temp")       .save('test-opi-330322.test.Base')       .mode("append")

    auxDate = auxDate + timedelta(days=1)
    # Saving the data to BigQuery
    


# In[ ]:




