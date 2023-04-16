# https://github.com/jonesberg/DataAnalysisWithPythonAndPySpark/blob/trunk/code/Ch03/word_count_submit.py
# VSCode shift+cmd+p -> Python: select interpretor -> select the one with PySpark


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName(
    "Count word occurences"
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

results = (
    spark.read.text("./data/gutenberg_books/*.txt") # relative to where your run spark-submit
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']+", 0).alias("word"))
        .where(F.col("word") != "")
        .groupBy(F.col("word"))
        .count()
)

results.orderBy("count", ascending=False).show(10)
results.coalesce(1).write.csv("./data/outputs/count_all_gutenberg_books.csv")
