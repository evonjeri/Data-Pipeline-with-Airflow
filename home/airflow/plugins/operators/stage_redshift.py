from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_field = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as json '{}'
    """


    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 copy_json_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id 
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.copy_json_option = copy_json_option
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        copy_query = f"COPY{self.table} FROM '{s3_path}' \
                     CREDENTIALS aws_iam_role={self.aws_credentials_id}' \
                     REGION '{self.region}' \
                     JSON '{self.copy_json_option}'; "
        
        redshift.run(copy_query)
        self.log.info(f"Successfully loaded data from s3 to {self.table} ")
        
        
        
"""
This operator is useful for loading data from S3 to Redshift.
1. This operator takes in
     task_id,
     dag, 
     table, 
     redshift_conn_id,
     aws_credentials_id, 
     s3_bucket, 
     s3_key, region, 
     copy_json_option as input. 
2. Connects to redshift using the redshift_conn_id

3. loads data from s3_bucket and s3_key into the table specified in the table attribute. 
Use the aws_credentials_id to access the S3 bucket, region to specify the region of the S3 bucket, and copy_json_option for loading the json file. 

"""





