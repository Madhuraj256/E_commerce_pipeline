import os
import sys
sys.path.insert(0,"/opt/project")
from pyspark.sql.functions import col,sum,countDistinct,max,desc,to_date,datediff,rank
from pyspark.sql.window import Window


def gold_func(spark):
    silver_path="/opt/project/data/silver"
    gold_path="/opt/project/data/gold"
    print("Gold Transformation started")

    customers = spark.read.format("delta").load(f"{silver_path}/customers")
    order= spark.read.format("delta").load(f"{silver_path}/order")
    order_item= spark.read.format("delta").load(f"{silver_path}/order_items")
    product= spark.read.format("delta").load(f"{silver_path}/product")
    payment= spark.read.format("delta").load(f"{silver_path}/order_payment")
    seller= spark.read.format("delta").load(f"{silver_path}/seller")
    review= spark.read.format("delta").load(f"{silver_path}/order_review")

    customers.filter(col("customer_id").isNull()).count()

    clv=order.join(order_item,on="order_id",how="inner").groupBy("customer_id").agg(countDistinct("order_id").alias("Total_orders"),sum("price").alias("Total_spent"),max("order_purchase_timestamp").alias("last_order_date")).orderBy("Total_spent",ascending=False)
    clv.write.format('delta').mode("overwrite").save(f"{gold_path}/customer_clv")
    
    daily_revenue=order.join(order_item,on="order_id",how="inner").groupBy(to_date("order_purchase_timestamp").alias("date")).agg(sum("price").alias("daily_revenue")).orderBy("daily_revenue")
    daily_revenue.write.format('delta').mode('overwrite').save(f"{gold_path}/daily_revenue")
    product_performance=order_item.groupBy("product_id").agg(sum("price").alias("revenue"),sum("freight_value").alias("shipping_cost")).orderBy(desc("revenue"),desc("shipping_cost"))
    product_performance.write.format('delta').mode("overwrite").save(f"{gold_path}/product_performance")
    delivery_speed=order.withColumn("delivery_days",datediff("order_purchase_timestamp","order_delivered_customer_date"))
    delivery_speed.write.format("delta").mode("overwrite").save(f"{gold_path}/delivery_performance")
    payment_analysis=payment.groupBy("payment_type").agg(sum("payment_value").alias("payment_value")).orderBy("payment_value",ascending=False)
    payment_analysis.write.format("delta").mode("overwrite").save(f"{gold_path}/payment_analysis")
    product_performance=product_performance.join(product.select("product_id","product_category_name"),on="product_id",how="left")
    window_spec=Window.partitionBy("product_category_name").orderBy(col("revenue").desc())
    Top_product=product_performance.withColumn("rank",rank().over(window_spec))
    Top_product.write.format("delta").mode("overwrite").save(f"{gold_path}/top_product")

    print("Gold tables: ",os.listdir(gold_path))

    print("Gold Transformation completed")





