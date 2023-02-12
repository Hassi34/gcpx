"""
Author : Hasanain Mehmood
Contact : hasanain@aicailber.com 
"""
from google.cloud import bigquery
import pandas as pd


class BigQueryGCP:
    
    """This class has all the methods for operations that needs to be performed with Big Query.
    Attributes:
            client (object): A BigQuery client object.
            project_id (str): GCP Project ID.
            dataset_name (str): Name of BigQuery dataset.
            data_table_name (str): Name of the BigQuery data table.
            dataset_id (str): ID of the BigQuery dataset.
            data_table_id (str): ID of the BigQuery data table.

    """
    def __init__(self, data_set_name:str, data_table_name:str, project_id: str=None):
        """Initializes the BQops class with the following parameters provided:

        Args:
            data_set_name (str): Name of BigQuery dataset.
            data_table_name (str): Name of the BigQuery data table.
            project_id (str, optional): GCP Project ID.Defaults to None.

        Raises:
            Exception : Raises Exception if could not find the dafult Project ID.
        """
        self.client = bigquery.Client()
        if project_id is None:
            try:
                #project_id = os.popen('gcloud config list project --format "value(core.project)"').read().strip()
                project_id = self.client.project
            except Exception as e:
                print(
                    "Could not find the Project ID. Please provide the Project ID manually")
                raise e
        self.project_id = project_id
        self.dataset_name = data_set_name
        self.data_table_name = data_table_name
        self.dataset_id = self.project_id+"."+self.dataset_name
        self.data_table_id = self.dataset_id+"."+self.data_table_name

    def create_bq_dataset(self, dataset_location: str="US"):
        """Creates a new BigQuery dataset

        Args:
            dataset_location (str, optional): A location of the data center to create the Big Query dataset in. Defaults to "US".
        """
        try:
            dataset = bigquery.Dataset(self.dataset_id)
            dataset.location = dataset_location
            # Make an API request.
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f'Created dataset "{self.dataset_id}"')
        except:
            print(
                f'BigQuery dataset "{self.dataset_name}" already exists in project "{self.project_id}"')
            print('Skipping dataset creation...')
            print('Note: a BiqQuery dataset should not contain white spaces')

    def create_bq_data_table(self, schema:list):
        """Creates a new BigQuery dataset

        Args:
            schema (list): Data table schema
        """
        if self.is_exist_ds():
            try:
                dataset_ref = bigquery.DatasetReference(
                    self.project_id, self.dataset_name)
                table_ref = dataset_ref.table(self.data_table_name)
                table = bigquery.Table(table_ref, schema=schema)
                table = self.client.create_table(table)
                print(
                    f'\nSuccessfully created data table "{self.data_table_name}" in dataset "{self.dataset_id}"')
            except:
                print(
                    f'Data table "{self.data_table_name}" already exists in dataset "{self.dataset_id}"')
                print('Skipping data table creation...')
        else:
            self.create_bq_dataset()
            self.create_bq_data_table(schema)

    def list_datasets(self)-> list:
        """Returns a list of available datasets in a project

        Returns:
            list: All available datasets
        """
        datasets = list(self.client.list_datasets())
        datasets = [dataset.dataset_id for dataset in datasets]
        return datasets
    
    def list_data_tables(self, dataset_name: str=None)-> list:
        
        """Returns a list of the BigQuery data tables available in a particular dataset ID
        
        Args:
            dataset_name (str, optional): Dataset Name to search the table in. Defaults to None.
        
        Returns:
            list: A list of data tables in a dataset.
        """
        try:
            if dataset_name:
                tables = self.client.list_tables(self.project_id+"."+dataset_name)
            else:
                tables = self.client.list_tables(self.dataset_id)
            tables = [table.table_id for table in tables]
            return tables
        except:
            print("Dataset is not available")

    def is_exist_dt(self)-> bool:
        """Checks is a table is available in a dataset ID

        Returns:
            bool: True if the data table is available else No.
        """
        if self.is_exist_ds():
            tables = self.list_data_tables()
            try:
                if self.data_table_name in tables:
                    return True
                else:
                    return False
            except TypeError:
                return False
        else:
            self.create_bq_dataset()
            self.is_exist_dt()

    def is_exist_ds(self)-> bool:
        """Checks is a dataset is available in a project ID

        Returns:
            bool: True if the data table is available else No.
        """
        datasets = list(self.client.list_datasets())
        datasets = [dataset.dataset_id for dataset in datasets]
        if self.dataset_name in datasets:
            return True
        else:
            return False

    def df_to_bq(self, df: pd.DataFrame, write_disposition: str="WRITE_TRUNCATE"):        
        """Inserts a pandas dataframe into a BiqQuery data table.

        Args:
            df (object): Pandas DataFrame object
            write_disposition (str, optional): BigQuery appends loaded rows to an existing table by default,
                                               but with WRITE_TRUNCATE write disposition it replaces the table
                                               with the loaded data. Defaults to "WRITE_TRUNCATE".

        Raises:
            Exception: Raises exception on failing the data write operation.
        """
        if self.is_exist_dt():
            try:
                print(
                    f'Found table name "{self.data_table_name}" in dataset "{self.dataset_id}"')
                job_config = bigquery.LoadJobConfig(
                    write_disposition=write_disposition)
                job = self.client.load_table_from_dataframe(
                    df, self.data_table_id, job_config=job_config
                )  # Make an API request.
                job.result()  # Wait for the job to complete.
                # Make an API request.
                table = self.client.get_table(self.data_table_id)
                print(
                    f'Loaded {len(df)} new rows and {len(table.schema)} columns to "{self.data_table_id}"')
                print(
                    f'Now, there are {table.num_rows} total rows and {len(table.schema)} columns in "{self.data_table_id}"')
            except Exception as e:
                print('Could not write the data to the table "{self.data_table_name}"')
                raise e
        else:
            print(
                f'Could not find Table name "{self.data_table_name}" in dataset "{self.dataset_id}"')
            print(
                f'Please call the method "create_bq_data_table" and pass the schema to create a data table')
    
    def get_df_by_query(self, query:str)-> pd.DataFrame:
        """Returns a Pandas dataframe on passing an SQL query

        Args:
            query (str): SQL query

        Returns:
            object: Pandas dataframe
        """          
        df = self.client.query(query).to_dataframe()
        return df
    
    def dlt_data_table(self, alert:bool=True):
        """Deletes a BigQuery data table

        Args:
            alert (bool, optional): If set to "True", it will ask for confirmation before deleting table. Defaults to True.
        """
        if alert:
            usr_rsp = input(f'>>> The data table {self.data_table_id} will be permanently deleted from {self.dataset_id}. Type "Yes" if you want to delete else "No" :')
            if usr_rsp.title() == "Yes":
                self.client.delete_table(self.data_table_id, not_found_ok=True)
                print(f'Deleted table "{self.data_table_id}".')
            elif usr_rsp.title() == "Yes":
                print('Did not drop the data table.')
            else:
                print("!!!Invalid Response!!!")
        else:
            self.client.delete_table(self.data_table_id, not_found_ok=True)
            print(f'Deleted table "{self.data_table_id}".')
