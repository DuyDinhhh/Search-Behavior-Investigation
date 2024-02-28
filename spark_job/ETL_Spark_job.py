import findspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from datetime import date, timedelta
import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
findspark.init()
sc = SparkContext('local')
spark = SparkSession(sc)

def generate_time_range_data(startdate, enddate):
    format = '%Y-%m-%d'
    day_time = []
    startdate = datetime.datetime.strptime(startdate, format).date()
    enddate = datetime.datetime.strptime(enddate, format).date()
    delta = enddate - startdate
    for i in range(delta.days + 1):
        day = startdate + timedelta(days=i)
        day = day.strftime('%Y%m%d')
        day_time.append(str(day))
    return day_time

def process_logsearch(startdate, enddate):
    path = "/Users/nguyentadinhduy/Documents/SQL_THLONG/DE_Gen5_Bigdata/class7/"
    day_range = generate_time_range_data(startdate, enddate)
    df = spark.read.parquet(path + day_range[0] + ".parquet")
    df = df.filter(df.user_id.isNotNull())
    df = df.select('user_id', 'keyword')
    for i in day_range[1:]:
        df1 = spark.read.parquet(path + i + ".parquet")
        df1 = df1.filter(df1.user_id.isNotNull())
        df1 = df1.select('user_id', 'keyword')
        df = df.union(df1)
    df = df.filter(df.keyword.isNotNull())
    return df

def ranking_data(process_result):
    window = Window.partitionBy("user_id").orderBy(col('keyword').desc())
    rank_result = process_result.withColumn('RANK', rank().over(window))
    rank_result = rank_result.filter(rank_result.RANK == '1').distinct()
    return rank_result

def categorize_keywords(df):
    category = spark.read.csv("/Users/nguyentadinhduy/Documents/SQL_THLONG/Projects/Project02/keysearch.csv", header=True, sep=";")
    result = df.join(category, category["Most_Search"] == df["keyword"], "inner")
    result = result.select("user_id","Most_Search","Category")
    return result

def change_Type(clean_data):
    clean_data = clean_data.withColumn("Customer_taste", when(col("category_june") == col("category_july"), "Unchanged")\
        .otherwise(concat_ws(" -> ",col("category_june"),col("category_july"))))
    return clean_data

def import_to_mysql(result):
    result.write.format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3307/data_engineering") \
    .option("dbtable", "user_search_data") \
    .mode("overwrite") \
    .option("user", "root") \
    .option("password", "root") \
    .save()
    return print('Data imported successfully')

def maintask():
    startdate_june = '2022-06-01'
    enddate_june = '2022-06-14'
    startdate_july = '2022-07-01'
    enddate_july = '2022-07-14'
    print('------------------------')
    print('Transforming data june')
    print('------------------------')
    summary_june = process_logsearch(startdate_june, enddate_june)
    summary_june = ranking_data(summary_june)
    print('------------------------')
    print('Showing some popular keywords in june')
    print('------------------------')
    summary_june.groupBy('keyword').count().orderBy(col('count').desc()).show(truncate=False)
    # summary_june = summary_june.withColumnRenamed("keyword", "most_search")
    summary_june = categorize_keywords(summary_june)
    summary_june = summary_june.withColumnRenamed("Most_Search", "most_search_june")
    summary_june = summary_june.withColumnRenamed("category", "category_june")
    print('------------------------')
    print('Transforming data july')
    print('------------------------')
    summary_july = process_logsearch(startdate_july, enddate_july)
    summary_july = ranking_data(summary_july)
    print('------------------------')
    print('Showing some popular keywords in july')
    print('------------------------')
    summary_july.groupBy('keyword').count().orderBy(col('count').desc()).show(truncate=False)
    # summary_july = summary_july.withColumnRenamed("keyword", "most_search")
    summary_july = categorize_keywords(summary_july)
    summary_july = summary_july.withColumnRenamed("Most_Search", "most_search_july")
    summary_july = summary_july.withColumnRenamed("category", "category_july")
    print('------------------------')
    print('Cooperating between June and July')
    print('------------------------')
    clean_data = summary_june.join(summary_july, "user_id", "inner")
    print('------------------------')
    print('Find currently taste')
    print('------------------------')
    clean_data = change_Type(clean_data)
    print('-----------------------------')
    print('Import result to mysql')
    print('-----------------------------')
    clean_data.show()
    import_to_mysql(clean_data)
    return print("Job finished")

maintask()
 
