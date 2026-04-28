import os
import sys
sys.path.insert(0,"/opt/project")
from jobs.utils.spark_session import get_spark_session
from src.bronze import ingest_bronze
from src.silver import transform_bronze_to_silver
from src.gold import gold_func


def main():
    spark = get_spark_session()
    print("=====started the Pipeline====")
    ingest_bronze(spark)
    print("===Bronze layer completed")
    transform_bronze_to_silver(spark)
    print("====Silver layer completed=========")
    gold_func(spark)
    print("=======Gold layer completed========")
    spark.stop()
    print("===== Pipeline Finished =====")

if __name__=="__main__":
    main()