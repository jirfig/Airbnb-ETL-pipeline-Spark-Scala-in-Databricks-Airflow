// Databricks notebook source
dbutils.widgets.dropdown(
  "scrape_year_month",
  "2021-01",
  Array("2021-01", "2021-02", "2021-03"),
  "scrape_year_month"
)
dbutils.widgets.text(
  "dim_model_container",
  "dim-model-azdb-notebook",
  "dim_model_container"
)
dbutils.widgets.text(
  "dim_model_container_new",
  "dim-model-azdb-notebook-new",
  "dim_model_container_new"
)
dbutils.widgets.text(
  "preprocessed_container",
  "preprocessed-notebook",
  "preprocessed_container"
)

// COMMAND ----------

val TEST = false
val scrape_year_month = dbutils.widgets.get("scrape_year_month")

val raw_data_container = "raw"        
val dim_model_container = dbutils.widgets.get("dim_model_container")
val dim_model_container_new = dbutils.widgets.get("dim_model_container_new")
val preprocessed_container = dbutils.widgets.get("preprocessed_container")

val storage_account = "jdata01"
val azure_path = s"wasbs://container@$storage_account.blob.core.windows.net"

val raw_global_listings_path = azure_path.replace("container", raw_data_container)+"/airbnb-listings.csv"
val raw_city_listings_path = azure_path.replace("container", raw_data_container)+s"/cities/*/$scrape_year_month/listings.csv"
val raw_city_reviews_path = azure_path.replace("container", raw_data_container)+s"/cities/*/$scrape_year_month/reviews.csv"
val raw_city_temperature_path = azure_path.replace("container", raw_data_container)+"/weather/ECA_blend_tg/*.txt"
val raw_city_rain_data_path = azure_path.replace("container", raw_data_container)+"/weather/ECA_blend_rr/*.txt"

val path_out_global_listings = azure_path.replace("container", preprocessed_container)+"/global_listings.parquet"
val path_out_city_listings_data = azure_path.replace("container", preprocessed_container)+s"/city_listings/$scrape_year_month/city_listings.parquet"     
val path_out_city_reviews_data = azure_path.replace("container", preprocessed_container)+s"/city_reviews/$scrape_year_month/city_reviews.parquet" 
val path_out_city_temperature_data = azure_path.replace("container", preprocessed_container)+"/city_temperature.parquet"
val path_out_city_rain_data = azure_path.replace("container", preprocessed_container)+"/city_rain.parquet"
val path_out_weather_stations = azure_path.replace("container", preprocessed_container)+"/weather_stations.parquet"  

val dim_model_listings = azure_path.replace("container", dim_model_container)+"/listings.csv"
val dim_model_hosts = azure_path.replace("container", dim_model_container)+"/hosts.csv"
val dim_model_reviews = azure_path.replace("container", dim_model_container)+"/reviews.csv"
val dim_model_reviewers = azure_path.replace("container", dim_model_container)+"/reviewers.csv"
val dim_model_weather = azure_path.replace("container", dim_model_container)+"/weather.csv"

val dim_model_listings_new = azure_path.replace("container", dim_model_container_new)+"/listings.csv"
val dim_model_hosts_new = azure_path.replace("container", dim_model_container_new)+"/hosts.csv"
val dim_model_reviews_new = azure_path.replace("container", dim_model_container_new)+"/reviews.csv"
val dim_model_reviewers_new = azure_path.replace("container", dim_model_container_new)+"/reviewers.csv"
val dim_model_weather_new = azure_path.replace("container", dim_model_container_new)+"/weather.csv"

val dim_model_reviews_step1 = azure_path.replace("container", dim_model_container_new)+"/reviews_step1.csv"
val dim_model_reviews_step2 = azure_path.replace("container", dim_model_container_new)+"/reviews_step2.csv"

val sc = spark.sparkContext
spark.conf.set(s"fs.azure.account.key.$storage_account.blob.core.windows.net", dbutils.secrets.get(scope = "JScope", key = "jdata01_key"))
sc.hadoopConfiguration.set(s"fs.azure.account.key.$storage_account.blob.core.windows.net", dbutils.secrets.get(scope = "JScope", key = "jdata01_key"))

// COMMAND ----------

val df_listings_global = spark.read.parquet(path_out_global_listings)

// COMMAND ----------

df_listings_global.count()

// COMMAND ----------

display(df_listings_global.limit(10))

// COMMAND ----------

val df_city_listings = spark.read.parquet(path_out_city_listings_data)

// COMMAND ----------

df_city_listings.count()

// COMMAND ----------

display(df_city_listings.limit(10))

// COMMAND ----------

val df_city_reviews = spark.read.parquet(path_out_city_reviews_data)

// COMMAND ----------

df_city_reviews.count()

// COMMAND ----------

display(df_city_reviews.limit(10))

// COMMAND ----------

val df_temp = spark.read.parquet(path_out_city_temperature_data)

// COMMAND ----------

df_temp.count()

// COMMAND ----------

display(df_temp.limit(100))

// COMMAND ----------

df_temp.createOrReplaceTempView("df_temp")
display(spark.sql("SELECT * FROM df_temp where STAID LIKE '%STAID%'"))

// COMMAND ----------

val df_rain = spark.read.parquet(path_out_city_rain_data)

// COMMAND ----------

df_rain.count()

// COMMAND ----------

display(df_rain.limit(65))

// COMMAND ----------

val df_stations = spark.read.parquet(path_out_weather_stations)

// COMMAND ----------

df_stations.count()

// COMMAND ----------

display(df_stations.limit(65))

// COMMAND ----------

val df_listings = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_listings_new)

// COMMAND ----------

df_listings.count()

// COMMAND ----------

display(df_listings.limit(10))

// COMMAND ----------

df_listings.select("listing_id").dropDuplicates("listing_id").count()

// COMMAND ----------

df_listings.select("listing_id").filter("listing_id IS NULL").show()

// COMMAND ----------

val df_hosts = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_hosts_new)

// COMMAND ----------

df_hosts.count()

// COMMAND ----------

display(df_hosts.limit(10))

// COMMAND ----------

df_hosts.select("host_id").dropDuplicates("host_id").count()

// COMMAND ----------

val df_weather = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_weather_new)

// COMMAND ----------

df_weather.count()

// COMMAND ----------

display(df_weather.limit(10))

// COMMAND ----------

df_weather.select("city").groupBy("city").count().show()

// COMMAND ----------

val df_reviews_step1 = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_reviews_step1)

// COMMAND ----------

df_reviews_step1.count()

// COMMAND ----------

display(df_reviews_step1.limit(10))

// COMMAND ----------

df_reviews_step1.select("review_id").dropDuplicates().count()

// COMMAND ----------

val df_reviews_step2 = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_reviews_step2)

// COMMAND ----------

df_reviews_step2.count()

// COMMAND ----------

display(df_reviews_step2.limit(10))

// COMMAND ----------

val df_reviews = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_reviews_new)

// COMMAND ----------

df_reviews.count()

// COMMAND ----------

df_reviews.select("review_id").dropDuplicates().count()

// COMMAND ----------

df_reviews.filter("comment_language is null").count()

// COMMAND ----------

display(df_reviews.limit(10))

// COMMAND ----------

df_reviews.groupBy("sentiment").count().show()

// COMMAND ----------

df_reviews.filter("sentiment == 'neg'").select("comments").limit(3).collect()

// COMMAND ----------

val df_reviewers = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_reviewers_new)

// COMMAND ----------

df_reviewers.count()

// COMMAND ----------

df_reviewers.select("reviewer_id").dropDuplicates("reviewer_id").count()

// COMMAND ----------

display(df_reviewers.limit(100))

// COMMAND ----------

// MAGIC %python
// MAGIC scrape_year_month = dbutils.widgets.get("scrape_year_month")   
// MAGIC dim_model_container = dbutils.widgets.get("dim_model_container")
// MAGIC dim_model_container_new = dbutils.widgets.get("dim_model_container_new")
// MAGIC preprocessed_container = dbutils.widgets.get("preprocessed_container")

// COMMAND ----------

// DBTITLE 1,Update dimensional model
// MAGIC %python
// MAGIC from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient   
// MAGIC blob_service_client = BlobServiceClient.from_connection_string(dbutils.secrets.get(scope = "JScope", key = "jdata01_connstr"))
// MAGIC 
// MAGIC storage_account = "jdata01"
// MAGIC raw_data_container = "raw"     
// MAGIC 
// MAGIC source_container_client = blob_service_client.get_container_client(dim_model_container_new)
// MAGIC target_container_client = blob_service_client.get_container_client(dim_model_container)
// MAGIC 
// MAGIC blobs_new_model = source_container_client.list_blobs()
// MAGIC try:
// MAGIC     blobs_new_model.next()
// MAGIC except StopIteration:
// MAGIC     raise ValueError("New model does not exist. Aborted.")
// MAGIC 
// MAGIC blobs_old_model = target_container_client.list_blobs()          
// MAGIC print("Deleting old model blobs if existing ...")    
// MAGIC for blob in blobs_old_model:
// MAGIC     old_model_blob = blob_service_client.get_blob_client(dim_model_container, blob.name)             
// MAGIC     old_model_blob.delete_blob()        
// MAGIC     print(f"{old_model_blob.url} deleted")   
// MAGIC 
// MAGIC blobs_list = source_container_client.list_blobs()
// MAGIC for blob in blobs_list:
// MAGIC     target_blob = blob_service_client.get_blob_client(dim_model_container, blob.name) 
// MAGIC     source_blob_url = f"https://{storage_account}.blob.core.windows.net/{dim_model_container_new}/"+blob.name
// MAGIC     target_blob.start_copy_from_url(source_blob_url)
// MAGIC     print(f"Copied {source_blob_url} to {target_blob.url}")
// MAGIC     source_blob = blob_service_client.get_blob_client(dim_model_container_new, blob.name) 
// MAGIC     source_blob.delete_blob()
// MAGIC     print(f"Deleted {source_blob_url}") 
