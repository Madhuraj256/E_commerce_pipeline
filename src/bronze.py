import os 
import sys
sys.path.insert(0, "/opt/project")
from pyspark.sql.functions import lit,current_timestamp


def ingest_bronze(spark):
 
    raw_path = "/opt/project/data/raw"
    bronze = "/opt/project/data/bronze"
 
    files = os.listdir(raw_path)
 
    for file in files:
        if file.endswith(".csv"):
            file_path = f"{raw_path}/{file}"
 
            table_name = file.replace(".csv", "").replace("olist_", "").replace("_dataset", "")
 
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            df = df.withColumn("ingestion_time", current_timestamp()).withColumn("sourcefile", lit(file))
 
            df.write.format("delta").mode("overwrite").save(f"{bronze}/{table_name}")
 
            print(f"✅ Written: {table_name}")
 
    print("\nBronze tables:", os.listdir(bronze))
 
