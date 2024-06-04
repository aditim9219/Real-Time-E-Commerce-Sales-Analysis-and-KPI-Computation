# Processing the data streams at input into the JSON files at output

# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Creating a spark session
spark = SparkSession \
        .builder \
        .appName("RetailDataAnalysis") \
        .getOrCreate()
        
# Setting log level to ERROR
spark.sparkContext.setLogLevel('ERROR')

# Reading Retail Dataset from Kafka topic : real-time-project
retail_data_raw = spark  \
              .readStream  \
              .format("kafka")  \
              .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
              .option("subscribe","real-time-project")  \
              .load()

# Defining the Data Structure for source retail data (json format).              
retail_data_JSON = StructType([
                            StructField("invoice_no", LongType()),
                            StructField("country", StringType()),
                            StructField("timestamp", TimestampType()),
                            StructField("type", StringType()),
                            StructField("items", ArrayType(StructType([
                                                                StructField("SKU", StringType()),
                                                                StructField("title", StringType()),
                                                                StructField("unit_price", FloatType()),
                                                                StructField("quantity", IntegerType())
                                                            ])))
                                ])

# Converting source data from json format to dataframe (table).            
retail_data_formatted =  retail_data_raw \
                 .select(from_json(col("value").cast("string"), retail_data_JSON).alias("retail_data")) \
                 .select("retail_data.*")

# Defining utility functions to generate metrics for each retail invoice: total_cost, total_items, is_order, is_return

# Checking whether new order
def order_checker(type):
   '''
        To check whether the type is Order or not
            1 - ORDER Type
            0 - Not an Order Type
    '''
   return 1 if type == 'ORDER' else 0

# Checking whether return order
def return_checker(type):
   '''
        To check whether the type is Return or not
            1 - Return Type
            0 - Not a Return Type
    '''   
   return 1 if type == 'RETURN' else 0
 
# Calculating Total Item Count in an order
def net_item_count(items):
    '''
        Calculating total no. of items present in each invoice (quantity).
    '''
    if items is not None:
        item_count =0
        for item in items:
            item_count = item_count + item['quantity']
        return item_count   

# Calculating Total Cost of an order
def net_cost(items,type):
    '''
        Calculating the total cost associated with each invoice.
        'total_cost' is the sum of '(unit_price * quantity)' for all items.
        If the invoice type is "Return," the total cost will be negative, representing the amount lost.
    ''' 
    if items is not None:
        total_cost =0
        item_price =0
    for item in items:
        item_price = (item['quantity']*item['unit_price'])
        total_cost = total_cost+ item_price
        item_price=0

    if type  == 'RETURN':
        return total_cost *-1
    else:
        return total_cost  

# Converting utility functions to UDFs
is_order = udf(order_checker, IntegerType())
is_return = udf(return_checker, IntegerType())
add_net_item_count = udf(net_item_count, IntegerType())
add_net_cost = udf(net_cost, FloatType())

# Extracting the columns/metrics: total_cost, total_items, is_order, and is_return from the source dataframe
retail_data_transformed = retail_data_formatted \
                          .withColumn("total_cost", add_net_cost(retail_data_formatted.items,retail_data_formatted.type)) \
                          .withColumn("total_items", add_net_item_count(retail_data_formatted.items)) \
                          .withColumn("is_order", is_order(retail_data_formatted.type)) \
                          .withColumn("is_return", is_return(retail_data_formatted.type))
                          

# Selecting the required columns and writing the retail transformed data to console
retail_data_sink = retail_data_transformed \
                  .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
                  .writeStream.outputMode("append").format("console").option("truncate", "false") \
                  .option("path", "/Console_output").option("checkpointLocation", "/Console_output_checkpoints") \
                  .trigger(processingTime="1 minute") \
                  .start()

# Calculating time based KPIs: OPM (Orders Per Minute), total_sale_volume, average_transaction_size, rate_of_return.
retail_time_kpi = retail_data_transformed \
                  .withWatermark("timestamp","1 minute") \
                  .groupBy(window("timestamp", "1 minute", "1 minute")) \
                  .agg(count("invoice_no").alias("OPM"),
                       sum("total_cost").alias("total_sale_volume"),
                       avg("total_cost").alias("average_transaction_size"),
                       avg("is_return").alias("rate_of_return")) \
                  .select("window", 
                          "OPM",
                          "total_sale_volume",
                          "average_transaction_size",
                          "rate_of_return")

# Writing retail data time based KPI in HDFS in JSON format
retail_time_kpi_sink = retail_time_kpi \
                       .writeStream \
                       .format("json") \
                       .outputMode("append") \
                       .option("truncate", "false") \
                       .option("path", "Timebased-KPI") \
                       .option("checkpointLocation", "Timebased-Checkpoint") \
                       .trigger(processingTime="1 minute") \
                       .start()

# Calculating time-country based KPIs: OPM (Orders Per Minute), total_sale_volume, rate_of_return.
retail_time_country_kpi = retail_data_transformed \
                  .withWatermark("timestamp","1 minute") \
                  .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
                  .agg(count("invoice_no").alias("OPM"),
                       sum("total_cost").alias("total_sale_volume"),
                       avg("is_return").alias("rate_of_return")) \
                  .select("window", 
                          "country",
                          "OPM",
                          "total_sale_volume",
                          "rate_of_return")

# Writing retail data time and country based KPI in HDFS in JSON format
retail_time_country_kpi_sink = retail_time_country_kpi \
                       .writeStream \
                       .format("json") \
                       .outputMode("append") \
                       .option("truncate", "false") \
                       .option("path", "Country-and-timebased-KPI") \
                       .option("checkpointLocation", "Country-and-timebased-Checkpoint") \
                       .trigger(processingTime="1 minute") \
                       .start()

# Executing the spark jobs for console outputs and storage
retail_data_sink.awaitTermination()
retail_time_kpi_sink.awaitTermination()
retail_time_country_kpi_sink.awaitTermination()
