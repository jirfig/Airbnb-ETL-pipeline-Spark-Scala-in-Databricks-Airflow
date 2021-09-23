package com.jirfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import com.jirfig.params.get_params


object process_reviews{
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

        // df_reviews_delta_joined = if (TEST) {
        //      df_reviews_delta_joined.limit(10000)
        // } else {
        //     df_reviews_delta_joined
        // }

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
    }

}  