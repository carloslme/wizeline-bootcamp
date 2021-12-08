# Final Deliverable 
(due November 28th, 11:59 PM)


Based on the self-study material, recorded and live session, and mentorship covered until this deliverable, we suggest you perform the following:
* Use your up and running Airflow Cluster to create your DAGs. 
* Think about the best way to design your data pipeline. Remember to include new concepts you are learning in previous weeks.
* Use terraform blocks to set up your Data Warehouse. Redshift in AWS and BigQuery in GCP.
* The goal is to run analytics from data stored in your cloud buckets (Staging Layer). For Redshift, you will need to configure Redshift Spectrum, and from BigQuery you will need to configure External Tables. 
* Add your IAM policies accordingly to connect your cloud services.
* Create a table in your Data Warehouse, using the schema attached (DW_Table.png)
* Remember to use terraform as much as possible.
* Use your EMR / Glue / Dataflow service to calculate user behavior metrics (User Behavior Metrics Logic)
* Feel free to run SQL Queries in your new DW table.


## User Behavior Metrics Logic:
* Data exported from PostgreSQL and stored in your Staging Layer will be named user_purchase.
* Classification data calculated from movie_review.csv and now stored in your Staging Layer will be named review.
* Data Warehouse will be populated following the logic below:
````
customerid   => user_purchase.customerid,
amount_spent => SUM(user_purchase.quantity * user_purchase.unit_price),
review_score => SUM(reviews.positive_review),
review_count => COUNT(reviews.id),                            
insert_date  => airflow timestamp
````

## Outcome:
* Terraform blocks to create your DW environment and the ability to run external queries. Also, IAM needed to integrate Airflow Cluster and Analytic tasks.
* Code where you run the User Behavior Metrics Logic in a distributed manner.
* (Optional) Automation process to run Terraform blocks as part of the main Data Pipeline

## Notes: 
What has been listed in this deliverable is just for guidance and to help you distribute your workload; you can deliver more or fewer items if necessary. However, if you deliver fewer items at this point, you have to cover the remaining tasks in the next deliverable.
Your mentor will post the feedback comments on your mentoring session. For reference, take a look at the DE Bootcamp calendar.
