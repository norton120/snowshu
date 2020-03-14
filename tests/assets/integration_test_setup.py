#!/usr/local/python3
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine
import yaml
import os
import sys

PROJECT_ROOT = os.getcwd()


def collect_tables():
    files=list()
    tables=set()
    for root,dirs,found_files in os.walk(os.path.join(PROJECT_ROOT,
                                                "tests",
                                                "assets",
                                                "data")):
        print(f'traversing {root} for files...')
        for name in found_files:
            if name.endswith('.csv'):
                files.append(os.path.join(PROJECT_ROOT,
                                          "tests",
                                          "data",
                                          root,
                                          name))
    print(f'found {len(files)} files.')
    for f in files:
        fileparts=f.split(os.path.sep)
        for part in fileparts:
            if part.startswith('SCHEMA='):
                schema=part.replace('SCHEMA=','')
        name=fileparts[-1].replace('.csv','')
        tables.add((f,schema.lower(),name.lower(),))
    print(f'found {len(tables)} tables.')
    return tables

def get_conn(profile, keyfile, database):

        if profile == 'bigquery':
            return create_engine(f'bigquery://{database}', credentials_path=keyfile)
        if profile == 'snowflake':
            return create_engine('snowflake://{}:{}@{}/{}'.format((creds['user'],
    creds['password'],
    creds['account'],
    database,)
))
        else:
            raise ValueError(f'{profile} not supported by setup script.')

def main(profile,
         creds_file):
    print(f'executing seed for {profile} with creds file {creds_file}')        
    database='snowshu-development' if profile == 'bigquery' else 'SNOWSHU_DEVELOPMENT' 
    with open(creds_file, 'r') as f:
        all_creds=yaml.load(f.read())['sources']
        keyfile=[x for x in all_creds if x['name'] == profile][0]['keyfile_path']
    print('collecting tables...')
    tables=collect_tables()
    print(f'collected {len(tables)} tables.')

    if profile == 'bigquery':
        print('connecting to bigquery...')
        client = bigquery.Client(project=database,
                                 credentials=service_account.Credentials.from_service_account_file(keyfile))
        print('connected')
        for dataset in set([x[1] for x in tables]):
            print(f'creating dataset {dataset} in bigquery..')
            dataset = bigquery.Dataset(f"{database}.{dataset}")
            dataset.location = "US"
            client.create_dataset(dataset)
            print('created')
    conn=get_conn(profile,keyfile,database)
    print(f'created connection {conn}')

    for table in tables:
        print(f'Creating table {table[2]}...')     
        pd.read_csv(table[0]).to_sql(table[2],
                                     conn,
                                     schema=table[1],
                                     chunksize=16000
                                     )
        print(f'Created table {table[2]}.')     

    ## make views 
    users_view=dict(name='users_view',schema='source_system',sql=f'select * from {database}.source_system.users')
    address_region_attributes_view=dict(name='address_region_attributes_view',schema='external_data',sql=f'select * from {database}.external_data.address_region_attributes')
    
    for view in (users_view,address_region_attributes_view,):
        logger.info(f'Creating view {view.name}...')
        create=f'CREATE VIEW {database}.{view.schema}.{view.name} AS {view.sql}'
        conn.execute(create)
        logger.info('Done creating view {view.name}.')

if __name__ == "__main__":
    main(*sys.argv[1:])


