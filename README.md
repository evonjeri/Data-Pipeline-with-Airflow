# Data-Pipeline-with-Airflow

PROJECT: DATA PIPELINE WITH AIRFLOW

Sparkify, a music streaming company has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and the best tool to achieve this is task is Apache Airflow.

As a data engineer, you are expected to create high grade data pipelines that are dynamic and built from reusable tasks, that can be monitored, and allow easy backfills. Since data quality plays a big part when analyses are executed on top the data warehouse, you are expected to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

PROJECT OVERVIEW

To complete the project, you will need to create your own custom operators to perform tasks such as:
	Staging the data. 

	Filling the data warehouse.

	Running checks on the data as the final step.

Project Files

1.	Project template that takes care of all the imports and provides dag template has all the imports and task templates in place, but the task dependencies have need to be set.
2.	The operators folder with operator template.
3.	Helpers class that contains all the SQL transformations. 

 
Prerequisites:
Create an IAM User in AWS.
•	Create a redshift cluster in AWS.
	Ensure that you are creating this cluster in the us-west-2 region. This is important as the s3-bucket that we are going to use for this project is in us-west-2.

Setting up Connections
•	Connect Airflow and AWS
•	Connect Airflow to the AWS Redshift Cluster.


Datasets
•	Log data: s3://udacity-dend/log_data

•	Song data: s3://udacity-dend/song_data
