package com.jirfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.jirfig.params.get_params


object process_listings_hosts{
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
    }

}    












    
    


        
        

