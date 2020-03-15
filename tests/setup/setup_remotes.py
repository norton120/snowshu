import yaml
import os
import sys
import google.api_core.exceptions as google_exceptions
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ROOT = os.getcwd()

SCHEMATA=(('SOURCE_SYSTEM',
            ('ORDERS',
             'ORDER_ITEMS',
             'PRODUCTS',
             'USERS',
             'USER_ADDRESSES',
             'USER_COOKIES',),),
          ('EXTERNAL_DATA',
            ('ADDRESS_REGION_ATTRIBUTES',
             'SOCIAL_USERS_IMPORT',),),
          ('TESTS_DATA',
            ('CASE_TESTING',
             'DATA_TYPES',),),)





def extract_profiles(file_path):
    with open(file_path,'r') as f:
        profiles=yaml.safe_load(f)['sources']
        return {x['adapter']:x for x in profiles}

def create_bigquery_datasets(conn):
    """ Can't create the project (database) datasets (schema) via ddl :("""
    for dataset in ('SOURCE_SYSTEM','EXTERNAL_DATA','TESTS_DATA',):
        dset_name=f"snowshu-development.{dataset}"
        conn.delete_dataset(dset_name,
                            delete_contents=True, 
                            not_found_ok=True)

        dset=bigquery.Dataset(dset_name)
        dset.location = "US"
        print(f'creating dataset {dataset}..')
        conn.create_dataset(dset)
        print(f'created dataset successfully.')

def make_bigquery_conn(file_path):
    print('making bigquery conn..')
    return bigquery.Client(project='snowshu-development',
                                 credentials=service_account.Credentials.from_service_account_file(file_path))

def load_csv_to_bigquery(conn,table,dataset):
    path=os.path.join(PROJECT_ROOT,
                      'tests',
                      'setup',
                      'data',
                      'DATABASE=SNOWSHU_DEVELOPMENT',
                      f'SCHEMA={dataset}',
                      f'{table.lower()}.csv')
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    with open(path,'rb') as csv:
        print(f'loading csv {path} into {dataset}.{table}..')
        conn.load_table_from_file(csv, 
                                  conn.dataset(dataset).table(table),
                                  job_config=job_config)
        print(f'csv loaded.')

def load_csvs_into_bigquery(conn):
    for schema, tables in SCHEMATA:
        for table in tables:
            load_csv_to_bigquery(conn,table,schema)

def main(creds_file_path):
    """ uses the creds file at file_path to create all the integration test
    environments available in that path"""
        
    def ddl_sql(prefix):
        filename=f'{prefix}_ddl.sql'
        return os.path.join(PROJECT_ROOT,'tests','setup','ddl_sql',filename)       

    available_profiles=extract_profiles(creds_file_path)
    
    for profile, creds in available_profiles.items():
        print(f'found profile {profile}')
        if profile == 'bigquery':
            conn=make_bigquery_conn(creds['keyfile_path'])
            create_bigquery_datasets(conn)
            with open(ddl_sql('bigquery')) as f:
                print('executing base ddl..')
                ddl_run=conn.query(f.read())
                print('executed')
            load_csvs_into_bigquery(conn)

if __name__ == '__main__':
    main(sys.argv[1])
