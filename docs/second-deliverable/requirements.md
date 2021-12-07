# Second Deliverable 
(due October 31st, 11:59 PM)

Based on the self-study material, recorded and live session, and mentorship covered until this deliverable, we suggest you perform the following:
* Use your up and running Airflow Cluster to create your DAGs. 
* Think about the best way to design your data pipeline. Remember to include new concepts you are learning in previous weeks.
* Use terraform blocks to set up your environment, you will need to create a new PostgreSQL DB with one table named user_purchase. (PostgreSQL_Table_Schema.png)
* Use Airflow to upload the user_purchase.csv file into the new PostgreSQL table. Remember there's more than 1 way to complete this step.

## Outcome:
* PostgreSQL table named user_purchase.
* Airflow job to load user_purchase.csv in user_purchase table.
* Terraform blocks to create PostgreSQL DB and table. Also, IAM needed to integrate Airflow Cluster and SQL service.
* (Optional) Automation process to run Terraform blocks as part of the main Data Pipeline

Notes: 
* What has been listed in this deliverable is just for guidance and to help you distribute your workload; you can deliver more or fewer items if necessary. However, if you deliver fewer items at this point, you have to cover the remaining tasks in the next deliverable.
* Your mentor will post the feedback comments on your mentoring session. For reference, take a look at the DE Bootcamp calendar.

## Resources
* [Implementation in AWS](https://github.com/wizelineacademy/data-engineering-bootcamp-tech-resources/tree/develop/week-02/bootcamp-demo-load-data-into-db)