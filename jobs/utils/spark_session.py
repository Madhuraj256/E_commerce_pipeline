from pyspark.sql import SparkSession

def get_spark_session():
    spark=SparkSession.builder\
        .appName("Brazillian Ecommerce")\
        .master("local[*]")\
        .config("spark.jars.packages","io.delta:delta-core_2.12:2.1.0")\
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark