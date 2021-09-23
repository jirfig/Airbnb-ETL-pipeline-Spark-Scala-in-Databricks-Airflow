// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.implicits._     
import org.apache.spark.sql.expressions.Window
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._

// COMMAND ----------

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

def model_exists(path: String):Boolean = {  
    try {
      spark.read.csv(path)
      true
    } catch {
      case e: Exception => false
    }      
 } 

// COMMAND ----------

// DBTITLE 1,Preprocess data
if  (!model_exists(path_out_global_listings)) {
    var df_global_listings = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true","sep"->";"))
                           .csv(raw_global_listings_path)

    val columns_global_listings = df_global_listings.columns.map(_.toLowerCase).map(_.replace(" ","_"))
    df_global_listings = df_global_listings.toDF(columns_global_listings:_*)

    val columns_to_drop = Seq("xl_picture_url", "cancellation_policy", "access", "features", "zipcode", "country_code", "smart_location",
                       "country", "security_deposit", "medium_url", "transit", "cleaning_fee", "street", "experiences_offered", 
                       "thumbnail_url", "extra_people", "weekly_price", "notes", "house_rules", "monthly_price", 
                       "summary", "square_feet", "interaction", "state","jurisdiction_names", "market", "geolocation", 
                       "space", "bed_type", "guests_included")

    df_global_listings = df_global_listings.drop(columns_to_drop:_*)         
    df_global_listings = df_global_listings.withColumn("scrape_year", year(col("last_scraped"))).withColumn("scrape_month", month(col("last_scraped")))


    if (TEST)    
        df_global_listings.filter("city = 'Amsterdam'").write.partitionBy("scrape_year","scrape_month").parquet(path_out_global_listings)
    else    
        df_global_listings.write.partitionBy("scrape_year","scrape_month").parquet(path_out_global_listings)

}

if  (!model_exists(path_out_city_listings_data)) {
    var df_city_listings = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                       .csv(raw_city_listings_path)
    df_city_listings = df_city_listings.withColumn("city",element_at(split(input_file_name(),"/"), -3))
    df_city_listings = df_city_listings.withColumn("scrape_year", year(col("last_scraped"))).withColumn("scrape_month", month(col("last_scraped")))

    if (TEST)    
        df_city_listings.filter("city = 'Amsterdam'").write.partitionBy("scrape_year","scrape_month").parquet(path_out_city_listings_data)
    else  
        df_city_listings.write.partitionBy("scrape_year","scrape_month").parquet(path_out_city_listings_data) 
}

if (!model_exists(path_out_city_reviews_data)) {
    var df_city_reviews = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                          .csv(raw_city_reviews_path)  
    df_city_reviews = df_city_reviews.withColumn("city",element_at(split(input_file_name(),"/"), -3))
    df_city_reviews = df_city_reviews.withColumn("year",year(col("date"))).withColumn("month", month(col("date")))

    if (TEST)    
        df_city_reviews.filter("city = 'Amsterdam'").write.partitionBy("year","month","city").parquet(path_out_city_reviews_data)
    else
        df_city_reviews.write.partitionBy("year","month","city").parquet(path_out_city_reviews_data)
}

if (!model_exists(path_out_city_temperature_data)) {  
   val df_temp = spark.read
         .option("mode", "DROPMALFORMED")         
         .schema("STAID String, SOUID String, DATE String, TG String, Q_TG String")
         .csv(raw_city_temperature_path)
         .filter("STAID != 'STAID'")

   df_temp.write.parquet(path_out_city_temperature_data)
}

if (!model_exists(path_out_city_rain_data)) {
   val df_rain = spark.read
         .option("mode", "DROPMALFORMED")         
         .schema("STAID String, SOUID String, DATE String, RR String, Q_RR String")
         .csv(raw_city_rain_data_path)
         .filter("STAID != 'STAID'")

   df_rain.write.parquet(path_out_city_rain_data)
}

if (!model_exists(path_out_weather_stations)) {
    val station_city = Seq((593,"Amsterdam"), (41,"Berlin"), (1860,"London"),(11249,"Paris"))
    val columns = Seq("STAID","city")
    val df_stations = spark.createDataFrame(station_city).toDF(columns:_*)  
    df_stations.write.parquet(path_out_weather_stations)
}

// COMMAND ----------

// DBTITLE 1,Process listings and hosts
val (df_listings, df_listings_hosts) = if (!model_exists(dim_model_listings)) { 
    var df_listings_global = spark.read.parquet(path_out_global_listings)
    df_listings_global.createOrReplaceTempView("global")
    val query="""
    SELECT *, cast(null as string) as bathrooms_text, cast(null as integer) as calculated_host_listings_count_entire_homes, cast(null as integer) as calculated_host_listings_count_private_rooms,
     cast(null as integer) as calculated_host_listings_count_shared_rooms, cast(null as string) as host_has_profile_pic, cast(null as string) as host_identity_verified, cast(null as string) as host_is_superhost,
     cast(null as string) as instant_bookable, cast(null as integer) as maximum_maximum_nights, cast(null as integer) as maximum_minimum_nights, cast(null as double) as maximum_nights_avg_ntm, cast(null as integer) as minimum_maximum_nights,
     cast(null as integer) as minimum_minimum_nights, cast(null as double) as minimum_nights_avg_ntm, cast(null as integer) as number_of_reviews_l30d, cast(null as integer) as number_of_reviews_ltm
    FROM global
    """

    var df_listings_hosts = spark.sql(query)
    val sorted_columns = df_listings_hosts.columns.sorted.map(c=>col(c))
    df_listings_hosts = df_listings_hosts.select(sorted_columns:_*)
    df_listings_hosts = df_listings_hosts.withColumnRenamed("id","listing_id")    

    // drop hosts columns from listings, except host_id
    val columns_to_drop = Seq("host_name", "host_url", "host_since", "host_location", "host_about", "host_response_time", "host_response_rate", "host_acceptance_rate",
    "host_is_superhost", "host_thumbnail_url", "host_picture_url", "host_neighbourhood", "host_listings_count",
    "host_total_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified")
    (df_listings_hosts.drop(columns_to_drop:_*), df_listings_hosts)    
    } else {
        (spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                   .csv(dim_model_listings),
        null)
}   


var df_listings_hosts_monthly = spark.read.parquet(path_out_city_listings_data)
val sorted_columns2 = df_listings_hosts_monthly.columns.sorted.map(c=>col(c))
df_listings_hosts_monthly = df_listings_hosts_monthly.select(sorted_columns2:_*)

// drop hosts columns from listings, except host_id
val columns_to_drop2 = Seq("host_name", "host_url", "host_since", "host_location", "host_about", "host_response_time", "host_response_rate", "host_acceptance_rate",
"host_is_superhost", "host_thumbnail_url", "host_picture_url", "host_neighbourhood", "host_listings_count",
"host_total_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified")
val df_listings_monthly = df_listings_hosts_monthly.drop(columns_to_drop2:_*).withColumnRenamed("id","listing_id")

// merge global and local listings, drop duplicates by filtering by latest scrape date
var df_listings_updated = df_listings.union(df_listings_monthly)
val windowSpec  = Window.partitionBy("listing_id").orderBy("last_scraped").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)                                             
df_listings_updated = df_listings_updated.withColumn("latest", last("last_scraped").over(windowSpec))
                  .filter("last_scraped == latest")
                  .dropDuplicates("listing_id")
                  .drop("latest")

df_listings_updated.write.options(Map("header"->"true", "escape"->"\""))
                         .csv(dim_model_listings_new)         


val df_hosts = if (!model_exists(dim_model_listings)) {        
    df_listings_hosts.createOrReplaceTempView("listings")
    val query="""
    SELECT host_id, host_name, host_url, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate,
    host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count,
    host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, last_scraped     
    FROM listings
    """
    var df_hosts = spark.sql(query)      

    val windowSpec  = Window.partitionBy("host_id").orderBy("last_scraped").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)                                             
    df_hosts.withColumn("latest", last("last_scraped").over(windowSpec))
            .filter("last_scraped == latest")
            .dropDuplicates("host_id")
            .drop("latest")
} else {
    spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
              .csv(dim_model_hosts)
}   

{
    // create hosts table
    df_listings_hosts_monthly.createOrReplaceTempView("listings")
    val query="""
    SELECT host_id, host_name, host_url, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate,
    host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count,
    host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, last_scraped     
    FROM listings
    """
    val df_hosts_monthly = spark.sql(query)      

    var df_hosts_updated = df_hosts.union(df_hosts_monthly)
    val windowSpec  = Window.partitionBy("host_id").orderBy("last_scraped").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)                                             
    df_hosts_updated = df_hosts_updated.withColumn("latest", last("last_scraped").over(windowSpec))
                       .filter("last_scraped == latest")
                       .dropDuplicates("host_id")
                       .drop("latest")

    df_hosts_updated.write.options(Map("header"->"true", "escape"->"\""))
                          .csv(dim_model_hosts_new) 
}

// COMMAND ----------

// DBTITLE 1,Process reviews
val df_reviews_monthly = spark.read.parquet(path_out_city_reviews_data)

val df_listings = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                        .csv(dim_model_listings_new)                   

var df_reviews_delta = if (!model_exists(dim_model_reviews)) {   
    df_reviews_monthly   
} else {
    val df_reviews = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                           .csv(dim_model_reviews)
    df_reviews.createOrReplaceTempView("reviews")
    df_reviews_monthly.createOrReplaceTempView("reviews_monthly")

    val query="""
    SELECT *
    FROM reviews_monthly
    WHERE reviews_monthly.date >= 
        (SELECT max(reviews.date)
         FROM reviews)   
    """
    spark.sql(query)
}       

df_reviews_delta.createOrReplaceTempView("reviews_delta")
df_listings.createOrReplaceTempView("listings")

val query="""
SELECT r.id as review_id, r.reviewer_id, r.listing_id, listings.host_id as host_id, concat_ws("_",r.city, r.date) as weather_id, r.date, r.reviewer_name, r.comments 
FROM reviews_delta r
LEFT JOIN listings
ON r.listing_id == listings.listing_id
"""
var df_reviews_delta_joined = spark.sql(query)

df_reviews_delta_joined.write.options(Map("header"->"true", "escape"->"\""))
                             .csv(dim_model_reviews_step1)

df_reviews_delta_joined = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                               .csv(dim_model_reviews_step1)

val language_detector = new PretrainedPipeline("detect_language_220", lang="xx")
val df_result = language_detector.transform(df_reviews_delta_joined.withColumnRenamed("comments","text"))
var df_reviews_delta2 = df_result.withColumn("comment_language", concat_ws(",",col("language.result"))).drop("document").drop("sentence").drop("language").withColumnRenamed("text","comments")

df_reviews_delta2.write.options(Map("header"->"true", "escape"->"\""))
                       .csv(dim_model_reviews_step2)     

df_reviews_delta2 = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                              .csv(dim_model_reviews_step2)

val sentiment_analyzer = PretrainedPipeline("analyze_sentimentdl_use_imdb", lang="en")
var df_result_sentiment = sentiment_analyzer.transform(df_reviews_delta2.filter("comment_language == 'en'").withColumnRenamed("comments","text"))
df_result_sentiment = df_result_sentiment.withColumn("sentiment", concat_ws(",",col("sentiment.result"))).drop("document").drop("sentence_embeddings").withColumnRenamed("text","comments")

val df_reviews_null = df_reviews_delta2.filter("comment_language is null").withColumn("sentiment", lit("n/a"))
val df_reviews_delta3 = df_reviews_delta2.filter("comment_language != 'en'").withColumn("sentiment", lit("n/a"))
                    .union(df_result_sentiment)
                    .union(df_reviews_null)

if (!model_exists(dim_model_reviews)) {       
    df_reviews_delta3.write.options(Map("header"->"true", "escape"->"\""))
                           .csv(dim_model_reviews_new)      
} else {
    val df_reviews = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                               .csv(dim_model_reviews)
    var df_reviews_updated = df_reviews.union(df_reviews_delta3)
    // Its necessary to drop duplicates since some of the reviews submitted at the scrape date will be included twice
    df_reviews_updated = df_reviews_updated.dropDuplicates("review_id")
    df_reviews_updated.write.options(Map("header"->"true", "escape"->"\""))
                            .csv(dim_model_reviews_new)
}

// COMMAND ----------

// DBTITLE 1,Process reviewers
val df_reviews = spark.read.options(Map("inferSchema"->"true","header"->"true", "multiLine"->"true", "escape"->"\"", "ignoreLeadingWhiteSpace"->"true"))
                       .csv(dim_model_reviews_new)

val windowSpec  = Window.partitionBy("reviewer_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)                                             
var df_reviewers = df_reviews
                           .withColumn("languages_spoken", collect_set("comment_language").over(windowSpec))
                           .withColumn("latest", last("date").over(windowSpec))
                           .filter("date == latest")
                           .dropDuplicates("reviewer_id")
                           .select("reviewer_id","reviewer_name", "languages_spoken", "date")
                           .withColumnRenamed("date","last_updated")
df_reviewers = df_reviewers.withColumn("languages_spoken",array_join(col("languages_spoken"),","))

df_reviewers.write.options(Map("header"->"true", "escape"->"\""))
                  .csv(dim_model_reviewers_new)  

// COMMAND ----------

// DBTITLE 1,Process weather
val df_temp = spark.read.parquet(path_out_city_temperature_data)
val df_rain = spark.read.parquet(path_out_city_rain_data)
val df_stations = spark.read.parquet(path_out_weather_stations)

df_temp.createOrReplaceTempView("temp")
df_rain.createOrReplaceTempView("rain")
df_stations.createOrReplaceTempView("stations")

val query="""
SELECT null as weather_id,to_date(temp.DATE, "yyyyMMdd") as date, temp.TG/10 as temperature, rain.RR/10 as rain, stations.city
FROM temp
JOIN rain
ON temp.DATE == rain.DATE
AND temp.STAID == rain.STAID
JOIN stations
ON temp.STAID == stations.STAID
WHERE to_date(temp.DATE, "yyyyMMdd") > to_date('20090101',"yyyyMMdd")
ORDER BY date
"""
var df_weather = spark.sql(query)
df_weather = df_weather.withColumn("weather_id",concat_ws("_",col("city"), col("date")))

df_weather.write.options(Map("header"->"true", "escape"->"\""))
                .csv(dim_model_weather_new)
