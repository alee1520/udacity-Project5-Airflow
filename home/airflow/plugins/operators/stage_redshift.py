from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    copy_sql_json = """
        truncate table {};
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """
    copy_sql_csv = """
        truncate table {};
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
        REGION '{}'
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_src="",
                 table_trunc="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 region="",
                 json="",                
                 ignore_headers=1,
                 file_type="",
                 *args, **kwargs):
                    
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_src = table_src
        self.table_trunc = table_trunc
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.region = region
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.json = json
        self.file_type = file_type

    def execute(self, context):
        self.log.info('StageToRedshiftOperator as started ....')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.file_type == 'json':
            self.log.info('Load json file from ' + s3_path )
            
            LOG_JSONPATH='s3://udacity-dend'        
            if self.json =='auto':
                LOG_JSONPATH = None     
            else:
                self.json = LOG_JSONPATH + self.json

            s3_copy_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.table_src,
                self.table_trunc,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.json
            )
        elif self.file_type =='csv':
            self.log.info('Load csv file from ' + s3_path )
            
            s3_copy_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.table_src,
                self.table_trunc,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter,
                self.region
            )
            
            self.log.info('Execute the following copy query:  ' + s3_copy_sql )
            
        redshift.run(s3_copy_sql)



