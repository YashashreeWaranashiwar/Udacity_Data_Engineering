from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 aws_credentials_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.aws_credentials_id=aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info("Perfoming Data Qaulity checks on table {table}")
            count_rows = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(count_rows) < 1 or len(count_rows[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_rows = count_rows[0][0]
            
            if num_rows < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {count_rows[0][0]} records") 
        