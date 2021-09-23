package com.jirfig
import org.apache.spark.sql.SparkSession


object params{
    def get_params(param:String, scrape_year_month:String="",dim_model_container:String="", dim_model_container_new:String="", config_file:String="") = {


        val storage_account = "jdata01"
        val raw_data_container = "raw"
        val preprocessed_container = "preprocessed"        
        val azure_path = s"wasbs://container@$storage_account.blob.core.windows.net"


        val result: String = param match {
            case "TEST" => "false"
            case "raw_data_container" => raw_data_container                                  
            case "preprocessed_container" => preprocessed_container
            case "storage_account" => storage_account
            case "azure_path" => azure_path
            case "raw_global_listings_path" => azure_path.replace("container", raw_data_container)+"/airbnb-listings.csv"
            case "raw_city_listings_path" => azure_path.replace("container", raw_data_container)+s"/cities/*/$scrape_year_month/listings.csv"
            case "raw_city_reviews_path" => azure_path.replace("container", raw_data_container)+s"/cities/*/$scrape_year_month/reviews.csv"
            case "raw_city_temperature_path" => azure_path.replace("container", raw_data_container)+"/weather/ECA_blend_tg/*.txt"
            case "raw_city_rain_data_path" => azure_path.replace("container", raw_data_container)+"/weather/ECA_blend_rr/*.txt"
            case "path_out_global_listings" => azure_path.replace("container", preprocessed_container)+"/global_listings.parquet"
            case "path_out_city_listings_data" => azure_path.replace("container", preprocessed_container)+s"/city_listings/$scrape_year_month/city_listings.parquet"     
            case "path_out_city_reviews_data" => azure_path.replace("container", preprocessed_container)+s"/city_reviews/$scrape_year_month/city_reviews.parquet" 
            case "path_out_city_temperature_data" => azure_path.replace("container", preprocessed_container)+"/city_temperature.parquet"
            case "path_out_city_rain_data" => azure_path.replace("container", preprocessed_container)+"/city_rain.parquet"
            case "path_out_weather_stations" => azure_path.replace("container", preprocessed_container)+"/weather_stations.parquet"  
            case "dim_model_listings" => azure_path.replace("container", dim_model_container)+"/listings.csv"
            case "dim_model_hosts" => azure_path.replace("container", dim_model_container)+"/hosts.csv"
            case "dim_model_reviews" => azure_path.replace("container", dim_model_container)+"/reviews.csv"
            case "dim_model_reviewers" => azure_path.replace("container", dim_model_container)+"/reviewers.csv"
            case "dim_model_weather" => azure_path.replace("container", dim_model_container)+"/weather.csv"
            case "dim_model_listings_new" => azure_path.replace("container", dim_model_container_new)+"/listings.csv"
            case "dim_model_hosts_new" => azure_path.replace("container", dim_model_container_new)+"/hosts.csv"
            case "dim_model_reviews_new" => azure_path.replace("container", dim_model_container_new)+"/reviews.csv"
            case "dim_model_reviewers_new" => azure_path.replace("container", dim_model_container_new)+"/reviewers.csv"
            case "dim_model_weather_new" => azure_path.replace("container", dim_model_container_new)+"/weather.csv"
            case "dim_model_reviews_step1" => azure_path.replace("container", dim_model_container_new)+"/reviews_step1.csv"
            case "dim_model_reviews_step2" => azure_path.replace("container", dim_model_container_new)+"/reviews_step2.csv"
            case "AZURE" => {
                val spark = SparkSession.builder()      
                                .getOrCreate();  
                val df_config = spark.read.option("header","true").csv(config_file)
                val key: String = df_config.filter(df_config("Key").equalTo(s"$storage_account"))
                                     .select("Value")
                                     .collect()(0)(0).toString
                key
                }                     
        }
        result
    }
}
