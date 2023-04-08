from utils import prepareData, evaluate, loadDataCsv
import pandas as pd
import numpy as np
import json
import sparknlp
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
import os

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, NGram, VectorAssembler, ChiSqSelector, StringIndexer
# from pyspark.ml.classification import LogisticRegression, NaiveBayes, DecisionTreeClassifier
from pyspark.ml.classification import LogisticRegression

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit


def LRModelPipeline():
    tokenizer = Tokenizer(inputCol="text", outputCol="token")
    hashtf = HashingTF(numFeatures=2**16, inputCol='token', outputCol='tf')
    idf = IDF(inputCol='tf', outputCol='features', minDocFreq=5)
    label_stringIdx = StringIndexer(inputCol = "label_id", outputCol = "label")

    lr = LogisticRegression(featuresCol='features', labelCol='label')
    paramGrid = (ParamGridBuilder() \
                 .addGrid(lr.regParam, [0.1, 0.01]) \
                 .addGrid(lr.fitIntercept, [False, True]) \
                 .build())
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='f1' )
    train_val_split = TrainValidationSplit(estimator=lr,
                           estimatorParamMaps=paramGrid,
                           evaluator=evaluator,
                           trainRatio=0.8)
    lrpipeline_tf_idf = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx, train_val_split])
    return lrpipeline_tf_idf

# Set enviroment
os.environ['PYSPARK_PYTHON'] = "I:/project/dataFromYtb/venv/Scripts/python.exe" 

if __name__ == "__main__":
    spark = SparkSession.builder.appName("load").getOrCreate()
    
    train_path = "model/data_tc/train.csv"
    dev_path = "model/data_tc/dev.csv"
    test_path = "model/data_tc/test.csv"
    train, test = prepareData(train_path, dev_path, test_path)
    lrModel_pipeline = LRModelPipeline().fit(train)
    lrModel_pipeline.write().overwrite().save("model/saved/lrModel_pipeline")
    lrModel_predictions = lrModel_pipeline.transform(test).select('text', 'label', 'prediction').toPandas()
    
    print(">>>>> Evaluate model:")
    evaluate(lrModel_predictions)
    # model = PipelineModel.read().load('model\saved\lrModel_pipeline')
    # pre = model.transform(data).select('text', 'label', 'prediction').toPandas()
    # evaluate(pre)



