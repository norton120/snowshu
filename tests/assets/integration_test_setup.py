#!/usr/local/python3
import pandas as pd
from sqlalchemy import create_engine
import yaml
import os
import sys

def collect_tables():
    files=list()
    tables=set()
    for root,dirs,files in os.walk(os.path.join(PROJECT_ROOT,
                                                "tests",
                                                "assets",
                                                "data")):
        for name in files:
            if name.endswith('.csv'):
                files.append(os.path.join(PROJECT_ROOT,
                                          "tests",
                                          "data",
                                          root,
                                          name)
    for f in files:
        fileparts=f.split(os.path.sep)
        for part in fileparts:
            if part.startswith('SCHEMA='):
                schema=part.replace('SCHEMA=','')
        name=fileparts[-1].replace('.csv','')
        tables.add((f,schema,name,))
    return tables

def get_conn(profile, creds_file):
    with open(creds_file, 'r') as f:
        creds=yaml.loads(f.read())['sources'][profile]
        if profile == 'bigquery':
            return create_engine('bigquery://SNOWSHU-DEVELOPMENT',
                                 credentials_path=creds['keyfile_path']
                                )
        if profile == 'snowflake':
            return create_engine('snowflake:://{}:{}@{}/SNOWSHU_DEVELOPMENT'.format((creds['user']
    creds['password'],
    creds['account'],)
)
        else:
            raise ValueError(f'{profile} not supported by setup script.')

def main(profile,
         creds_file):
        
    tables=collect_tables()
    conn=get_conn(profile,creds_file)

    for table in tables:
        print(f'Creating table {table[2]}...')     
        pd.read_csv(table[0]).to_sql(table[2],
                                     conn,
                                     schema=table[1],
                                     chunksize=16000
                                     )
        print(f'Created table {table[2]}.')     

    ## make views 
    users_view=dict(name='USERS_VIEW',schema='SOURCE_SYSTEM',sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."USERS"')
    address_region_attributes_view=dict(name='address_region_attributes_view',schema='EXTERNAL_DATA',sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."EXTERNAL_DATA"."ADDRESS_REGION_ATTRIBUTES"')
    
    for view in (users_view,address_region_attributes_view,):
        logger.info(f'Creating view {view.name}...')
        create=f'CREATE VIEW "SNOWSHU_DEVELOPMENT"."{view.schema}"."{view.name}" AS {view.sql}'
        conn.execute(create)
        logger.info('Done creating view {view.name}.')

if __name__ == "__main__":
    main(sys.argval[1:])


