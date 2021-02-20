from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_statement,
                 target_table,
                 redshift_connection_id,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.target_table = target_table
        self.redshift_connection_id = redshift_connection_id

    def execute(self, context):
        self.log.info('Inside LoadFactOperator -->>>>')
        # Creating a Postgres hook for redshift database
        redshift = PostgresHook(self.redshift_connection_id)
        
        # Formatting the insert sql with the target table
        sql = self.sql_statement.format(self.target_table)
        #Executing the data load into the target table
        self.log.info('Going to perform insert into fact table {}'.format(self.target_table))
        redshift.run(sql)
        self.log.info('Data loaded from staging table to {} fact table'.format(self.target_table))