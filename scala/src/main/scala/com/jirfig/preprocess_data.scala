package com.jirfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.jirfig.params.get_params


object preprocess_data {
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
    }
}    











// filter syntax https://sparkbyexamples.com/spark/spark-dataframe-where-filter/
    
    


        
        

