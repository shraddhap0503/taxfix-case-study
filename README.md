# TaxFix Case Study

## Getting Started

*  Clone the repo and cd into the folder


    `git clone https://github.com/shraddhap0503/taxfix-case-study.git`
   
    `cd taxfix-case-study/`

* Build the Spark Docker image
  
   `docker-compose build`

* Start Spark with Docker

   `docker-compose up`

* Run the API Data Ingestion Python Program to fetch the data from Faker_API
    
   `docker exec -it spark_app python /main/api_data_ingestion.py`

* Run the Spark Job

  `docker exec -it spark_app spark-submit /main/data_transformation.py`
  

* View the Output Files

  `docker cp spark_app:/resources/output/cleansed/. ./output_docker`

* Queries for genertaing the reports can be found in the **SQL_Queries** file under the project root directory.  
