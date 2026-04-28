import os
import sys
sys.path.insert(0,"/opt/project")
from pyspark.sql.functions import col, sum as spark_sum,to_timestamp

def transform_bronze_to_silver(spark):
    print("Transforming Bronze to Silver...")
    bronze_path="/opt/project/data/bronze"
    silver_path="/opt/project/data/silver"
    files = os.listdir(silver_path)

    df=spark.read.format('delta').load(f"{bronze_path}/customers")
    df.select([spark_sum(col(c).isNull().cast("int")).alias(c)for c in df.columns]).show()
    clean_df=df.dropDuplicates(["customer_id"])
    clean_df=clean_df.toDF(*[c.strip().lower().replace(" ","_") for c in clean_df.columns])
    clean_df.write.format("delta").mode("overwrite").save("/opt/project/data/silver/customers")

    order_df=spark.read.format("delta").load(f"{bronze_path}/orders")
    order_df=order_df.toDF(*[o.strip().lower().replace(" ","_") for o in order_df.columns])
    clean_df=order_df.filter(col("order_id").isNotNull() & col("customer_id").isNotNull()).dropDuplicates(["order_id"])
    date_columns=[
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"]

    for date in date_columns:
          clean_df=clean_df.withColumn(date,col(date).cast("timestamp"))

    clean_df.write.format("delta").mode("overwrite").save(f"{silver_path}/order")

    order_items_df=spark.read.format("delta").load(f"{bronze_path}/order_items")
    clean_df_1=order_items_df.toDF(*[c.strip().lower().replace(" ","_") for c in order_items_df.columns])
    clean_df=clean_df_1.dropDuplicates(["order_id","order_item_id"])
    clean_df.write.format("delta").mode("overwrite").save(f"{silver_path}/order_items")

    
    order_payment_df=spark.read.format('delta').load(f"{bronze_path}/order_payments")
    order_payment_df=order_payment_df.toDF(*[o.strip().lower().replace(" ","_")for o in order_payment_df.columns])
    order_payment_df.write.format("delta").save(f"{silver_path}/order_payment")

    order_reviews_df=spark.read.format('delta').load(f"{bronze_path}/order_reviews")
    order_reviews_df=order_reviews_df.toDF(*[c.strip().lower().replace(" ","_") for c in order_reviews_df.columns])
    order_reviews_df=order_reviews_df.dropDuplicates(["review_id","order_id"])
    dates=[
    "review_creation_date",
    "review_answer_timestamp"]

    for date in dates:
        order_reviews_df=order_reviews_df.withColumn(date,col(date).cast("timestamp"))

    order_reviews_df.write.format('delta').mode("overwrite").save(f"{silver_path}/order_review")

    products_df=spark.read.format('delta').load(f"{bronze_path}/products")
    products_df=products_df.toDF(*[c.strip().lower().replace(" ","_") for c in products_df.columns])
    products_df.write.format('delta').mode("overwrite").save(f"{silver_path}/product")

    product_category_name_translation_df=spark.read.format('delta').load(f"{bronze_path}/product_category_name_translation")
    product_category_name_translation_df=product_category_name_translation_df.toDF(*[c.strip().lower().replace(" ","_") for c in product_category_name_translation_df.columns])
    product_category_name_translation_df.write.format('delta').mode("overwrite").save(f"{silver_path}/product_category_name_translation")

    sellers_df=spark.read.format('delta').load(f"{bronze_path}/sellers")
    sellers_df=sellers_df.toDF(*[c.strip().lower().replace(" ","_") for c in sellers_df.columns])
    sellers_df.select([spark_sum(col(o).isNull().cast("int")).alias(o)for o in sellers_df.columns]).show()
    sellers_df.write.format('delta').mode("overwrite").save(f"{silver_path}/seller")

    geolocation_df=spark.read.format('delta').load(f"{bronze_path}/geolocation")
    geolocation_df=geolocation_df.dropDuplicates(["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"])
    geolocation_df.write.format('delta').mode("overwrite").save(f"{silver_path}/geolocation")

    print(f"silver tables:{os.listdir(silver_path)}")

    print("✅ Bronze to Silver transformation completed!")


