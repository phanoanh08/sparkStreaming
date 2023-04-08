from connectDatabase import mysqlConnector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import os

os.environ['PYSPARK_PYTHON'] = "I:/project/dataFromYtb/venv/Scripts/python.exe" 

if __name__ == "__main__":
    spark_ss = SparkSession.builder.appName("partition").getOrCreate()
    mydb = mysqlConnector("oanh", "123456a_", "commentOnYtb")
    cursor = mydb.cursor(buffered=True)
    cursor.execute("select * from videoCmt")
    columns_name = [i[0] for i in cursor.description]
    dataframe = pd.DataFrame(cursor.fetchall(), columns=columns_name)    
    dataframe.to_csv("tmp/data.csv", header=True, index=True, encoding='utf8')
    dataframe['cmt'] = dataframe['cmt'].apply(lambda x: x.replace("\n", " ").replace("\t", " ").replace("\r", " ").replace("\"", "").strip())
    dataframe.to_csv("tmp/data_.csv", header=True, index=True, encoding='utf8')
    print(dataframe.iloc[337, :])
    print(dataframe.iloc[383, :])
    dataframe['year'] = dataframe['updateAt'].apply(lambda x: x.strftime("%Y"))
    dataframe['month'] = dataframe['updateAt'].apply(lambda x: x.strftime("%m"))
    dataframe['date'] = dataframe['updateAt'].apply(lambda x: x.strftime("%d"))
    print(">>>>> Dataframe's info")
    print(dataframe.info())
    dataframe.to_csv("tmp/database.csv", header=True, index=False, encoding='utf8')
    sdf = spark_ss.read.option("header", True).csv("tmp/database.csv")
    print(">>>>> Dataframe's schema")
    sdf.printSchema()

    sdf.write.option("header", True).mode("overwrite") \
                                    .partitionBy(["year", "month", "date"]) \
                                    .format("csv") \
                                    .save("data")
    
    