# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
import clickhouse_connect
os.environ["JAVA_HOME"] = 'c:\\Program Files\\Java\\jdk-22\\'
os.environ["HADOOP_HOME"] = 'c:\\hadoop-3.4.0\\'

CRE_PG_DB = 'postgresdb'
CRE_PG_TB = 'users'
CRE_PG_US = 'postgresadmin'
CRE_PG_PS = 'admin123'
CRE_CL_DB = 'default'
CRE_CL_TB = 'users'
CRE_CL_US = 'default'
CRE_CL_PS = ''

spark = SparkSession.builder.appName('PostgreSQL PySpark').config("spark.jars","libs/postgresql-42.7.3.jar").getOrCreate()

# Получим данные из таблицы PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/" + CRE_PG_DB) \
    .option("dbtable", CRE_PG_TB) \
    .option("user", CRE_PG_US) \
    .option("password", CRE_PG_PS) \
    .option("driver", "org.postgresql.Driver") \
    .load()

dataCollect = df.collect()
# Если данные есть начнем "миграцию"
if (len(dataCollect)):
    client = clickhouse_connect.get_client(host='localhost', port=8123, username=CRE_CL_US, password=CRE_CL_PS, database=CRE_CL_DB)
    for row in dataCollect:
        query_result = client.query(f"INSERT INTO {CRE_CL_TB} (id, name, age, salary) VALUES ({row['id']},\'{row['name']}\',{row['age']},{row['salary']});")

    query_result = client.query('SELECT * FROM ' + CRE_CL_TB)
    print('В результирующей таблице записей:', len(query_result.result_set))
    client.close()
else:
    print('Нет данных для миграции')