from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

scrape_year_month = '{{ execution_date.strftime("%Y-%m") }}'
storage_account = "jdata01"
raw_data_container = "raw"
preprocessed_container = "preprocessed"     
dim_model_container = "dim-model-azdb"
dim_model_container_new = "dim-model-azdb-new"   
azure_path = f"wasbs://container@{storage_account}.blob.core.windows.net"

raw_global_listings_path = azure_path.replace("container", raw_data_container)+"/airbnb-listings.csv"
raw_city_listings_path = azure_path.replace("container", raw_data_container)+f"/cities/*/{scrape_year_month}/listings.csv"
raw_city_reviews_path = azure_path.replace("container", raw_data_container)+f"/cities/*/{scrape_year_month}/reviews.csv"
raw_city_temperature_path = azure_path.replace("container", raw_data_container)+"/weather/ECA_blend_tg/*.txt"
raw_city_rain_data_path = azure_path.replace("container", raw_data_container)+"/weather/ECA_blend_rr/*.txt"
path_out_global_listings = azure_path.replace("container", preprocessed_container)+"/global_listings.parquet"
path_out_city_listings_data = azure_path.replace("container", preprocessed_container)+f'/city_listings/{scrape_year_month}/city_listings.parquet'
path_out_city_reviews_data = azure_path.replace("container", preprocessed_container)+f'/city_reviews/{scrape_year_month}/city_reviews.parquet'
path_out_city_temperature_data = azure_path.replace("container", preprocessed_container)+"/city_temperature.parquet"
path_out_city_rain_data = azure_path.replace("container", preprocessed_container)+"/city_rain.parquet"
path_out_weather_stations = azure_path.replace("container", preprocessed_container)+"/weather_stations.parquet"  
dim_model_listings = azure_path.replace("container", dim_model_container)+"/listings.csv"
dim_model_hosts = azure_path.replace("container", dim_model_container)+"/hosts.csv"
dim_model_reviews = azure_path.replace("container", dim_model_container)+"/reviews.csv"
dim_model_reviewers = azure_path.replace("container", dim_model_container)+"/reviewers.csv"
dim_model_weather = azure_path.replace("container", dim_model_container)+"/weather.csv"
dim_model_listings_new = azure_path.replace("container", dim_model_container_new)+"/listings.csv"
dim_model_hosts_new = azure_path.replace("container", dim_model_container_new)+"/hosts.csv"
dim_model_reviews_new = azure_path.replace("container", dim_model_container_new)+"/reviews.csv"
dim_model_reviewers_new = azure_path.replace("container", dim_model_container_new)+"/reviewers.csv"
dim_model_weather_new = azure_path.replace("container", dim_model_container_new)+"/weather.csv"
dim_model_reviews_step1 = azure_path.replace("container", dim_model_container_new)+"/reviews_step1.csv"
dim_model_reviews_step2 = azure_path.replace("container", dim_model_container_new)+"/reviews_step2.csv"

jar_path = "dbfs:/FileStore/jars/airbnb-etl_2.12-1.0.jar"

default_args = {
    'owner': 'jirfig',
    'start_date': datetime(2021, 1, 1),
    'end_date': datetime(2021, 3, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,    
    'depends_on_past': False    
}

dag = DAG('airbnb-databricks',
  schedule_interval = '@monthly',
  default_args = default_args,
  catchup = True,
  description='Airbnb ETL pipeline: Spark(Scala) in Databricks & Airflow',
  max_active_runs=1
  )

cluster_specs = {        
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',    
    'num_workers': 1,
    "autoscale" : {
        "min_workers": 1,
        "max_workers": 9
        }
    }

def path_exists(*op_args):
    "Check if blobs in Azure exist"
    wasb_hook = WasbHook('wasb_default')
    for path in op_args:
        val = wasb_hook.check_for_prefix(container_name=path.replace("@","/").split("/")[2],prefix="/".join(path.replace("@","/").split("/")[4::]))           
        if val == False:
            raise ValueError(f"Path {path} is empty") 


def update_dim_model():
    "Delete the dimensional model and move in new dimensional model from 'temporary' folder"
    wasb_hook = WasbHook('wasb_default')
    blob_service_client = wasb_hook.connection
    source_container_client = blob_service_client.get_container_client(dim_model_container_new)
    target_container_client = blob_service_client.get_container_client(dim_model_container)

    blobs_new_model = source_container_client.list_blobs()
    try:
        blobs_new_model.next()
    except StopIteration:
        raise ValueError("New model does not exist. Aborted.")


    blobs_old_model = target_container_client.list_blobs()          
    print("Deleting old model blobs if existing ...")    
    for blob in blobs_old_model:
        old_model_blob = blob_service_client.get_blob_client(dim_model_container, blob.name)             
        old_model_blob.delete_blob()        
        print(f"{old_model_blob.url} deleted")   

    blobs_list = source_container_client.list_blobs()
    for blob in blobs_list:
        target_blob = blob_service_client.get_blob_client(dim_model_container, blob.name) 
        source_blob_url = f"https://{storage_account}.blob.core.windows.net/{dim_model_container_new}/"+blob.name
        target_blob.start_copy_from_url(source_blob_url)
        print(f"Copied {source_blob_url} to {target_blob.url}")
        source_blob = blob_service_client.get_blob_client(dim_model_container_new, blob.name) 
        source_blob.delete_blob()
        print(f"Deleted {source_blob_url}") 


start_operator = DummyOperator(
    task_id='begin_execution',     
    dag=dag)

preprocess_data_submit = DatabricksSubmitRunOperator(
    task_id = 'preprocess_data_submit',
    databricks_conn_id = 'databricks_default',
    spark_jar_task = {'main_class_name': 'com.jirfig.preprocess_data',
                      'parameters': ['{{ execution_date.strftime("%Y-%m") }}',dim_model_container,dim_model_container_new,"dbfs:/FileStore/config/prj_cfg.csv"]},
    new_cluster = cluster_specs,
    libraries=[{'jar': jar_path}], 
    dag=dag
)

preprocess_data_check = PythonOperator(
    task_id='preprocess_data_check', 
    python_callable=path_exists, 
    op_args=[path_out_global_listings,
             path_out_city_listings_data,
             path_out_city_reviews_data,
             path_out_city_temperature_data,
             path_out_city_rain_data,
             path_out_weather_stations],      
    dag=dag    
)

process_listings_hosts_submit = DatabricksSubmitRunOperator(
    task_id = 'process_listings_hosts_submit',
    databricks_conn_id = 'databricks_default',
    spark_jar_task = {'main_class_name': 'com.jirfig.process_listings_hosts',
                      'parameters': ['{{ execution_date.strftime("%Y-%m") }}',dim_model_container,dim_model_container_new,"dbfs:/FileStore/config/prj_cfg.csv"]},
    new_cluster = cluster_specs,
    libraries=[{'jar': jar_path}], 
    dag=dag
)

listings_hosts_check = PythonOperator(
    task_id='listings_hosts_check', 
    python_callable=path_exists,  
    op_args=[dim_model_listings_new,
             dim_model_hosts_new],      
    dag=dag    
)

process_reviews_submit = DatabricksSubmitRunOperator(
    task_id = 'process_reviews_submit',
    databricks_conn_id = 'databricks_default',
    spark_jar_task = {'main_class_name': 'com.jirfig.process_reviews',
                      'parameters': ['{{ execution_date.strftime("%Y-%m") }}',dim_model_container,dim_model_container_new,"dbfs:/FileStore/config/prj_cfg.csv"]},
    new_cluster = cluster_specs,
    libraries=[{'jar': jar_path},
               {"maven": {"coordinates": "com.johnsnowlabs.nlp:spark-nlp_2.12:3.2.2"}}], 
    dag=dag
)

reviews_check = PythonOperator(
    task_id='reviews_check', 
    python_callable=path_exists,  
    op_args=[dim_model_reviews_new],      
    dag=dag    
)

process_reviewers_submit = DatabricksSubmitRunOperator(
    task_id = 'process_reviewers_submit',
    databricks_conn_id = 'databricks_default',
    spark_jar_task = {'main_class_name': 'com.jirfig.process_reviewers',
                      'parameters': ['{{ execution_date.strftime("%Y-%m") }}',dim_model_container,dim_model_container_new,"dbfs:/FileStore/config/prj_cfg.csv"]},
    new_cluster = cluster_specs,
    libraries=[{'jar': jar_path}], 
    dag=dag
)

reviewers_check = PythonOperator(
    task_id='reviewers_check', 
    python_callable=path_exists,  
    op_args=[dim_model_reviewers_new],      
    dag=dag    
)

process_weather_submit = DatabricksSubmitRunOperator(
    task_id = 'process_weather_submit',
    databricks_conn_id = 'databricks_default',
    spark_jar_task = {'main_class_name': 'com.jirfig.process_weather',
                      'parameters': ['{{ execution_date.strftime("%Y-%m") }}',dim_model_container,dim_model_container_new,"dbfs:/FileStore/config/prj_cfg.csv"]},
    new_cluster = cluster_specs,
    libraries=[{'jar': jar_path}], 
    dag=dag
)

weather_check = PythonOperator(
    task_id='weather_check', 
    python_callable=path_exists,  
    op_args=[dim_model_weather_new],      
    dag=dag    
)

update_dim_model = PythonOperator(
    task_id='update_dim_model', 
    python_callable=update_dim_model,
    dag=dag)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> preprocess_data_submit >> preprocess_data_check

preprocess_data_check >> process_listings_hosts_submit >> listings_hosts_check 
listings_hosts_check >> process_reviews_submit >> reviews_check
reviews_check >> process_reviewers_submit >> reviewers_check >> update_dim_model
preprocess_data_check >> process_weather_submit >> weather_check >> update_dim_model 

update_dim_model >> end_operator