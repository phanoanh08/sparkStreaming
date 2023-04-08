# import pyspark
# from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import sparknlp
from sklearn.metrics import f1_score, roc_auc_score, accuracy_score
   

def cleaningProcess(data, text_col):
    user_regex = r"(@\w{1,15})"
    hashtag_replace_regex = "#(\w{1,})"
    url_regex = r"((https?|ftp|file):\/{2,3})+([-\w+&@#/%=~|$?!:,.]*)|(www.)+([-\w+&@#/%=~|$?!:,.]*)"
    email_regex = r"[\w.-]+@[\w.-]+\.[a-zA-Z]{1,}"
    i_regex = r"i "
        # Loại bỏ @Mention khỏi text
    data = (data.withColumn(text_col,f.regexp_replace(f.col(text_col), user_regex, ""))
        # Loại bỏ #Hashtag khỏi text
             .withColumn(text_col,f.regexp_replace(f.col(text_col), hashtag_replace_regex, "$1"))
        # Loại bỏ URL khỏi text
            .withColumn(text_col,f.regexp_replace(f.col(text_col), url_regex, "")) 
        # Loại bỏ Email khỏi text
            .withColumn(text_col,f.regexp_replace(f.col(text_col), email_regex, ""))
        # Chuẩn hoá viết thường
            .withColumn(text_col,f.lower(f.col(text_col)))
        # Loại bỏ số cũng như các ký tự khỏi đoạn text
            .withColumn(text_col,f.regexp_replace(f.col(text_col), '[^a-záàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệóòỏõọôốồổỗộơớờởỡợíìỉĩịúùủũụưứừửữựýỳỷỹỵđ]', " "))
         # Loại bỏ các khoảng trắng thừa trong câu
            .withColumn(text_col,f.regexp_replace(f.col(text_col), " +", " "))
        # Loại bỏ các khoảng trắng đầu và cuối câu
            .withColumn(text_col,f.trim(f.col(text_col)))
        # Giữ lại các dòng mà đoạn text có nội dung 
            .filter(f.col(text_col) != ""))
    return data

def loadDataCsv(data_path):
    spark_nlp = sparknlp.start()
    df = spark_nlp.read \
      .option("multiline", "true") \
      .option("quote", '\"') \
      .option("header", "true") \
      .option("escape", "\"") \
      .csv(data_path)
    return df

def prepareData(train, dev, test):
    df_train = loadDataCsv(train)
    df_dev = loadDataCsv(dev)
    df_test = loadDataCsv(test)
    
    df_train = df_train.union(df_dev)
    df_train = df_train.withColumnRenamed('free_text', 'text')
    df_test = df_test.withColumnRenamed('free_text', 'text')     

    train = cleaningProcess(df_train, "text")
    test = cleaningProcess(df_test, "text")

    return train, test

def evaluate(data):
    print('>>>>>accuracy: ', accuracy_score(data['label'], data['prediction']))
    print('>>>>>f1_score: ', f1_score(data['label'], data['prediction'], average='macro'))


