from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 sql_statement,
                 target_table,
                 redshift_connection_id,
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.target_table = target_table
        self.truncate = truncate
        self.redshift_connection_id = redshift_connection_id

    def execute(self, context):
        self.log.info('Inside LoadDimensionOperator -->>>>')
        # Creating a Postgres hook for redshift database
        redshift = PostgresHook(self.redshift_connection_id)
        
        # Check if truncate boolean is switched ON to delete data from the target table
        if self.truncate :
            self.log.info('Going to perform delete into {}'.format(self.target_table))
            delete_sql = "Delete from {}".format(self.table_name)
            redshift.run(delete_sql)
            elf.log.info('Data deleted from {}'.format(self.target_table))
        
        # Formatting the insert sql with the target table
        sql = self.sql_statement.format(self.target_table)
        #Executing the data load into the target table
        self.log.info('Going to perform insert into {}'.format(self.target_table))
        redshift.run(sql)
        self.log.info('Data loaded from staging table to {} dimension table'.format(self.target_table))
