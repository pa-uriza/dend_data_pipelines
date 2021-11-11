from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_statement="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_statement = insert_statement
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_statement
        )
        
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info(f"Loading dmension table '{self.table}' into Redshift")
        redshift.run(formatted_sql)
