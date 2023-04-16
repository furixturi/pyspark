import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main(spark, file_path="./data/gutenberg_books/*.txt", top_N=5):
    results = (
        spark.read.text(file_path)
            .select(F.split(F.col("value"), " ").alias("line"))
            .select(F.explode(F.col("line")).alias("word"))
            .select(F.lower(F.col("word")).alias("word"))
            .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
            .where(F.col("word") != "")
            .groupBy(F.col("word"))
            .count()
    )
    
    results.orderBy("count", ascending=False).show(top_N)
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName("WordOccurencesRank").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    top_N = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    main(spark, file_path, top_N)
    
    # Good practice to terminate the spark context
    spark.stop()