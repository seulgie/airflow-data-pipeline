from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        REGION '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 json_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option

    def execute(self, context):

        self.log.info(f'Staging data from S3 to Redshift table: {self.table}')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)

        # Format S3 path using Airflow template
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        # Fetch IAM Role from AWS credentials
        aws_conn = s3_hook.get_connection(self.aws_credentials_id)
        iam_role_arn = aws_conn.extra_dejson.get("role_arn")

        if not iam_role_arn:
            raise ValueError("IAM Role ARN not found in AWS connection extras")
        
        self.log.info(f'Using IAM Role: {iam_role_arn}')

        # Clear existing data in the table
        self.log.info(f'Clearing data from Redshift table {self.table}')
        redshift.run(f"DELETE FROM {self.table}")

        # Run COPY command
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            iam_role_arn,
            self.region,
            self.json_option
        )
        self.log.info(f'Executing COPY command: {formatted_sql}')
        redshift.run(formatted_sql)
        self.log.info(f'Successfully staged {self.table} from S3 to Redshift.')
