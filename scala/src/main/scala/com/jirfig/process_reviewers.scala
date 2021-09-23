package com.jirfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.jirfig.params.get_params



object process_reviewers{
  def main(args: Array[String]){
      val spark = SparkSession.builder()      
                              .getOrCreate();    
      import spark.implicits._

      def model_exists(path: String):Boolean = {  
          try {
            spark.read.csv(path)
            true
          } catch {
            case e: Exception => false
          }      
       } 

      val scrape_year_month = args(0)
      val dim_model_container = args(1)
      val dim_model_container_new = args(2)
      val config_file = args(3)
      
      val TEST = if (get_params("TEST") == "true") true else false
      val raw_data_container = get_params("raw_data_container",scrape_year_month,dim_model_container,dim_model_container_new)
      val preprocessed_container = get_params("preprocessed_container",scrape_year_month,dim_model_container,dim_model_container_new)
                   
      val storage_account = get_params("storage_account",scrape_year_month,dim_model_container,dim_model_container_new)
      
      val raw_global_listings_path = get_params("raw_global_listings_path",scrape_year_month,dim_model_container,dim_model_container_new)
      val raw_city_listings_path = get_params("raw_city_listings_path",scrape_year_month,dim_model_container,dim_model_container_new)
      val raw_city_reviews_path = get_params("raw_city_reviews_path",scrape_year_month,dim_model_container,dim_model_container_new)
      val raw_city_temperature_path = get_params("raw_city_temperature_path",scrape_year_month,dim_model_container,dim_model_container_new)
      val raw_city_rain_data_path = get_params("raw_city_rain_data_path",scrape_year_month,dim_model_container,dim_model_container_new)

      val path_out_global_listings = get_params("path_out_global_listings",scrape_year_month,dim_model_container,dim_model_container_new)
      val path_out_city_listings_data = get_params("path_out_city_listings_data",scrape_year_month,dim_model_container,dim_model_container_new)
      val path_out_city_reviews_data = get_params("path_out_city_reviews_data",scrape_year_month,dim_model_container,dim_model_container_new)
      val path_out_city_temperature_data = get_params("path_out_city_temperature_data",scrape_year_month,dim_model_container,dim_model_container_new)
      val path_out_city_rain_data = get_params("path_out_city_rain_data",scrape_year_month,dim_model_container,dim_model_container_new)
      val path_out_weather_stations = get_params("path_out_weather_stations",scrape_year_month,dim_model_container,dim_model_container_new)

      val dim_model_listings = get_params("dim_model_listings",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_hosts = get_params("dim_model_hosts",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_reviews = get_params("dim_model_reviews",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_reviewers = get_params("dim_model_reviewers",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_weather = get_params("dim_model_weather",scrape_year_month,dim_model_container,dim_model_container_new)

      val dim_model_listings_new = get_params("dim_model_listings_new",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_hosts_new = get_params("dim_model_hosts_new",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_reviews_new = get_params("dim_model_reviews_new",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_reviewers_new = get_params("dim_model_reviewers_new",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_weather_new = get_params("dim_model_weather_new",scrape_year_month,dim_model_container,dim_model_container_new)

      val dim_model_reviews_step1 = get_params("dim_model_reviews_step1",scrape_year_month,dim_model_container,dim_model_container_new)
      val dim_model_reviews_step2 = get_params("dim_model_reviews_step2",scrape_year_month,dim_model_container,dim_model_container_new)

      val key = get_params("AZURE",config_file=config_file)

      val sc = spark.sparkContext
      spark.conf.set(s"fs.azure.account.key.$storage_account.blob.core.windows.net", key)
      sc.hadoopConfiguration.set(s"fs.azure.account.key.$storage_account.blob.core.windows.net", key)


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
  }

}  