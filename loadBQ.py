#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession, SQLContext, Row
from datetime import datetime, timedelta
from pyspark.sql.functions import *


# In[2]:


# Create a SparkSession under the name "reddit". Viewable via the Spark UI
spark = SparkSession.builder.appName("testSession").getOrCreate()


# In[11]:


startDate = auxDate = datetime.strptime('2021-01-01', '%Y-%m-%d').date()
endDate= datetime.strptime('2021-01-01', '%Y-%m-%d').date()
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
        df1.createOrReplaceTempView('df1')

        aux = spark.sql('SELECT MAX(id) as max from df1').collect()[0]

        idMax = int(aux['max'])
    except:        
        idMax=0
    df = df.filter(df['id'] > idMax)
    df.show(truncate=False)


    auxDate = auxDate + timedelta(days=1)
    #df.write.mode(SaveMode.Append).format('bigquery').option("temporaryGcsBucket","ds1-dataproc/temp").insertInto('test-opi-330322.test.Base2')
      #.save('test-opi-330322.test.Base2')
    df.write   .format("bigquery")   .option("temporaryGcsBucket","ds1-dataproc/temp")   .save("test-opi-330322.test.Base3")


    


# In[ ]:




