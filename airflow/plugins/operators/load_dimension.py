from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
    This operator is able to load data into Dimension table 
    by executing specified SQL statement for specified target table.

"""

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    """
        Inputs:
        * redshift_conn_id Redshift connection ID in Airflow connectors
        * sql_stat SQL statement for loading data into target Fact table
        * target_table name of target Fact table to load data into
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 sql_stat = "",
                 target_table = "",
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_stat = sql_stat
        self.target_table = target_table

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info("Starting insert data into dimension table: {}".format(self.target_table))
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.sql_stat)
        self.log.info("Done inserting data into dimension table: {}".format(self.target_table))
