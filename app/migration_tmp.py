# -*- coding: utf-8 -*-
#import psycopg2
import os
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import clickhouse_connect
# import csv
# import findspark
# findspark.add_packages('clickhouse:mysql-connector-java:8.0.11')
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

def pg_connect():
    spark = SparkSession.builder.appName('PostgreSQL PySpark').config("spark.jars", "libs/postgresql-42.7.3.jar").getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/"+CRE_PG_DB) \
        .option("dbtable", CRE_PG_TB) \
        .option("user", CRE_PG_US) \
        .option("password", CRE_PG_PS) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df

def ch_connect():
    client = clickhouse_connect.get_client(host='localhost', port=8123, username=CRE_CL_US, password=CRE_CL_PS, database=CRE_CL_DB)
    query_result = client.query('SELECT * FROM '+CRE_CL_TB)
    print(query_result.result_set)

    #return df

# def connect_to_clickhouse_with_pyspark():
#     # DBMS: ClickHouse(ver.24.6.2.17)
#     # Case sensitivity: plain = exact, delimited = exact
#     # Driver: ClickHouse JDBC
#     # Driver(ver.0.6.0 - patch5(revision: 067ae0b), JDBC4.2)
#     # Ping: 25 ms
#
#     #.config("spark.driver.extraClassPath", "./clickhouse-jdbc-0.6.0-patch5-all.jar") \
#     spark = SparkSession.builder \
#         .appName('Clickhouse PySpark') \
#         .config("spark.jars", "libs/clickhouse-jdbc-0.6.0-patch5-all.jar") \
#         .getOrCreate()
#
#     df = spark.read \
#         .format("jdbc") \
#         .option("url", "jdbc:clickhouse://localhost:8123/"+CRE_CL_DB) \
#         .option("dbtable", CRE_CL_TB) \
#         .option("user", CRE_CL_US) \
#         .option("password", "") \
#         .load()
#
#     return df

def pg_fetch():
    df = pg_connect()
    return df

# def save_to_csv(data, filename):
#     with open(filename, 'w', newline='') as csvfile:
#         writer = csv.writer(csvfile)
#         for row in data:
#             writer.writerow(row)

#print('Данные из таблицы PostgreSQL:')
df = pg_fetch()
#Тут можно было в CSV сохранить, но стандартная функция выдает ошибку... не смог победить на WINDOWS на centos нормально работала

dataCollect = df.collect()
if (len(dataCollect)):
    client = clickhouse_connect.get_client(host='localhost', port=8123, username=CRE_CL_US, password=CRE_CL_PS, database=CRE_CL_DB)
    for row in dataCollect:
        print(str(row['id']) + "," +row['name'] + "," +str(row['age']) + "," +str(row['salary']))
        query_result = client.query('INSERT INTO '+CRE_CL_TB+' (id, name, age, salary) VALUES ('+str(row['id'])+',\''+row['name'] +'\','+str(row['age']) +','+str(row['salary'])+'),')

    query_result = client.query('SELECT * FROM ' + CRE_CL_TB)
    print(query_result.result_set)
    client.close()

#data = postgresql_get_data()
#save_to_csv(data, 'tmp/postgresql_data.csv')
print('Данные из таблицы ClickHouse:')
ch_connect()