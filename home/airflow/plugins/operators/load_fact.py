from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_template ="{}"    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_nm="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_nm = query_nm

    def execute(self, context):
        
        self.log.info('LoadFactOperator: start fact table load')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_sql = LoadFactOperator.sql_template.format(self.query_nm)
        self.log.info('Execute the follwoing fact sql: ' + fact_sql)
        redshift.run(fact_sql)       
