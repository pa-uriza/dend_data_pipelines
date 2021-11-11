from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        tests_failed = []
        
        for check in self.checks:
            sql = check.get('sql')
            expected = check.get('expected')

            try:
                self.log.info(f"Running query: {sql}")
                records = redshift_hook.get_records(sql)[0]
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")
                tests_failed.append(sql)
                continue

            if expected != records[0]:
                tests_failed.append(sql)
        else:
            self.log.info("All data quality checks passed")

        if tests_failed:
            self.log.info('Tests failed')
            self.log.info(tests_failed)
            raise ValueError('One or more quality checks failed')
        