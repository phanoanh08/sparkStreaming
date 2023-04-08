from model.utils import loadDataCsv, cleaningProcess
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.pipeline import PipelineModel
import os




def pathConfig():
    model_path = "model/saved/lrModel_pipeline"
    ipt_dstream_path = "sdata"
    return model_path, ipt_dstream_path

def label(value, label):
    if label=="clean":
        if value==0:
            return 1
        else:
            return 0
    elif label=="offensive":
        if value==1:
            return 1
        else:
            return 0
    elif label=="hate":
        if value==2:
            return 1
        else:
            return 0
    else:
        raise Exception("Label isnot set!")

def detectLabel(model_path, dstream_obj):
    model = PipelineModel.read().load(model_path)
    dstream_obj = cleaningProcess(dstream_obj, "text")

    results = model.transform(dstream_obj) \
                .select("video_id", "update_at", "author_id", "text", "prediction") \
                .withColumnRenamed("prediction", "label")
    
    udf_0 = udf(lambda x: label(x, "clean"))
    udf_1 = udf(lambda x: label(x, "offensive"))
    udf_2 = udf(lambda x: label(x, "hate"))
    results = results.withColumn("clean", udf_0(results.label))
    results = results.withColumn("offensive", udf_1(results.label))
    results = results.withColumn("hate", udf_2(results.label))

    return results

os.environ['PYSPARK_PYTHON'] = "I:/project/dataFromYtb/venv/Scripts/python.exe" 

if __name__ == "__main__":
    model_path, ipt_dstream = pathConfig()

    spark = SparkSession.builder.appName("streamData") \
                            .getOrCreate()
    
    schema = StructType([StructField("video_id", StringType(), True),
                        StructField("update_at", StringType(), True),
                        StructField("author_id", StringType(), True),
                        StructField("text", StringType(),True),
                        StructField("label", IntegerType(), True),])
    
    customer = spark.readStream \
                .format("csv") \
                .schema(schema) \
                .option("header", True) \
                .option("maxFilePerTrigger", 1) \
                .load(ipt_dstream)
    
    results = detectLabel(model_path, customer)

    average_salary = results.groupBy("video_id") \
                        .agg(sum("hate").alias("hate"),
                             sum("offensive").alias("offensive"),
                             sum("clean").alias("clean"),
                             count("video_id").alias("total")) 


    query = average_salary.writeStream \
                    .format("console") \
                    .outputMode("complete") \
                    .start() \
                    .awaitTermination()


