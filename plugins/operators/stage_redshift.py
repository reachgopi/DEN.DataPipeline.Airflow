from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    stage_load_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        TIMEFORMAT as 'epochmillisecs'
        
    """
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_connection_id,
                 table_name,
                 aws_credential_id,
                 s3_bucket,
                 s3_key,
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_connection_id = redshift_connection_id
        self.aws_credential_id = aws_credential_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info('Inside StageToRedshiftOperator -->>>')
        s3_path = "s3://{}/{}"
        delete_sql = "Delete from {}".format(self.table_name)
        # Using AWS Hook to get aws credentials from Airflow
        aws_hook = AwsHook(self.aws_credential_id)
        aws_credentials = aws_hook.get_credentials()
        # Using PostgresHook to get Redshift connection details from Airflow
        redshift = PostgresHook(self.redshift_connection_id)
        # Replaces with the execution date and month using JINJA Templates
        updated_key = self.s3_key.format(**context)
        s3_location = s3_path.format(self.s3_bucket, updated_key)
        # Formatting the sql statements with actual table name, aws cred details
        stage_load_sql = StageToRedshiftOperator.stage_load_sql.format(
                    self.table_name,
                    s3_location,
                    aws_credentials.access_key,
                    aws_credentials.secret_key,
                    self.json_path
                )
        # Executing delete sql statement
        redshift.run(delete_sql)
        self.log.info("Deleted data from staging table {}".format(self.table_name))
        # Executing load sql statement
        redshift.run(stage_load_sql)
        self.log.info("Data loaded successfully to staging table {}".format(self.table_name))