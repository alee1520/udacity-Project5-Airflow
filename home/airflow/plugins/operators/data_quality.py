from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    sql_template = "{}"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_nm="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_nm = query_nm

    def execute(self, context):
        self.log.info('Apply data quality check')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        dq_sql = DataQualityOperator.sql_template.format(self.query_nm)      
        self.log.info('Execute the following data quality query: ' + dq_sql)
        
        records = redshift.get_records(dq_sql)
        self.log.info(records)