from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    sql_template ="{}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_nm="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.query_nm = query_nm

    def execute(self, context):
        self.log.info('LoadDimensionOperator executing...')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        dim_sql = LoadDimensionOperator.sql_template.format(self.query_nm)
        self.log.info('Execute the following dimension query: ' + dim_sql)

        redshift.run(dim_sql)
    