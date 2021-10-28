#!/usr/bin/env python
# coding: utf-8

# In[21]:


from pyspark.sql import SparkSession, SQLContext, Row
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from google.cloud import bigquery


# In[22]:


dataframe = spark.read.parquet("gs://ds1-dataproc/archivos/out/devices.parquet/year=2021/month=1/day=1")


# In[23]:


startDate = auxDate = datetime.strptime('2021-01-01', '%Y-%m-%d').date()
endDate= datetime.strptime('2021-01-02', '%Y-%m-%d').date()
print(startDate.year)


# In[24]:


while auxDate < endDate:
    year = auxDate.year
    month = auxDate.month
    day = auxDate.day
    #Datos para 3 dias antes
    year3 = (auxDate - timedelta(days=3)).year
    month3 = (auxDate - timedelta(days=3)).month
    day3 = (auxDate - timedelta(days=3)).day
    try:
        df = spark.read.parquet("gs://ds1-dataproc/archivos/out/devices.parquet/year={0}/month={1}/day={2}".format(year,month,day))
        df3 = spark.read.parquet("gs://ds1-dataproc/archivos/out/devices.parquet/year={0}/month={1}/day={2}".format(year3,month3,day3))
        
    except:
        pass
    # Cargas de Temporales
    df.createOrReplaceTempView('df')
    
    df.show(truncate=False)
    auxDate = auxDate + timedelta(days=1)
    # Saving the data to BigQuery
#table = f"test-330322:test.sf_prueba"
#table_df = (spark.read.format('bigquery').option('table', table).load())
    


# In[ ]:




