import time
import pandas as pd
from typing import Tuple,List,Union,Any,Optional
from snowshu.core.models.attribute import Attribute
from snowshu.core.models.relation import Relation
from snowshu.adapters.source_adapters import BaseSourceAdapter
from snowshu.adapters.source_adapters.sample_methods import SampleType,BernoulliSample, SystemSample
import snowshu.core.models.data_types as dtypes
import snowshu.core.models.materializations as mz
from snowshu.logger import Logger
from snowshu.core.models.credentials import Credentials,USER,PASSWORD,ACCOUNT,DATABASE,SCHEMA,ROLE,WAREHOUSE
logger=Logger().logger

class SnowflakeAdapter(BaseSourceAdapter):
    
    def __init__(self):
        super().__init__()    
    
    SUPPORTED_SAMPLE_METHODS=(BernoulliSample,SystemSample)
    REQUIRED_CREDENTIALS=(USER,PASSWORD,ACCOUNT,DATABASE,)
    ALLOWED_CREDENTIALS=(SCHEMA,WAREHOUSE,ROLE,)

    DATA_TYPE_MAPPINGS=dict(number=dtypes.INTEGER,
                            float=dtypes.DOUBLE,
                            text=dtypes.VARCHAR,
                            boolean=dtypes.BOOLEAN,
                            date=dtypes.DATE,
                            timestamp_ntz=dtypes.TIMESTAMP,
                            timestamp_ltz=dtypes.TIMESTAMPTZ,
                            timestamp_tz=dtypes.TIMESTAMPTZ,
                            variant=dtypes.JSON,
                            object=dtypes.OBJECT,
                            array=dtypes.ARRAY,
                            binary=dtypes.BINARY)
    
    MATERIALIZATION_MAPPINGS={"BASE TABLE":mz.TABLE,
                              "VIEW":mz.VIEW}

    ##TODO: this is the future, move buz logic to base and replace with these.
    GET_ALL_DATABASES_SQL=  """ SELECT DISTINCT database_name 
                                FROM "UTIL_DB"."INFORMATION_SCHEMA"."DATABASES"
                                WHERE is_transient = 'NO'
                                AND database_name <> 'UTIL_DB'"""

    def unsampled_statement(self,relation:Relation)->str:
        return f"""
SELECT
    *
FROM
    {relation.quoted_dot_notation}
"""

    def directionally_wrap_statement(self,sql:str,sample_type:Union[SampleType,None])->str:
        if sample_type is None:
            return sql

        return f"""
WITH
    __SNOWSHU_FINAL_SAMPLE AS (
{sql}
)
,___SNOWSHU_DIRECTIONAL_SAMPLE AS (
SELECT
    *
FROM
    __SNOWSHU_FINAL_SAMPLE
{self._sample_type_to_query_sql(sample_type)}
)
SELECT 
    *
FROM 
    __SNOWSHU_DIRECTIONAL_SAMPLE
"""

    def analyze_wrap_statement(self,sql:str,relation:Relation)->str:
        return f"""
WITH
    __SNOWSHU_COUNT_POPULATION AS (
SELECT
    COUNT(*) AS population_size
FROM
    {relation.quoted_dot_notation}
)
,__SNOWSHU_CORE_SAMPLE AS (
{sql}
)
,__SNOWSHU_CORE_SAMPLE_COUNT AS (
SELECT
    COUNT(*) AS sample_size
FROM
    __SNOWSHU_CORE_SAMPLE
)
SELECT
    s.sample_size AS sample_size
    ,p.population_size AS population_size
FROM
    __SNOWSHU_CORE_SAMPLE_COUNT s
INNER JOIN
    __SNOWSHU_COUNT_POPULATION p
ON
    1=1
LIMIT 1
"""

    def sample_statement_from_relation(self,relation:Relation,sample_type:Union[SampleType,None])->str:
        """builds the base sample statment for a given relation"""
        query=f"""
SELECT
    *
FROM 
    {relation.quoted_dot_notation} 
"""
        if sample_type is not None:
            query+=f"{self._sample_type_to_query_sql(sample_type)}"
        return query

    def predicate_constraint_statement(self,relation:Relation,analyze:bool,local_key:str,remote_key:str)->str:
        """builds 'where' strings"""

        constraint_sql=str()
        if analyze:
            constraint_sql=relation.core_query
        else:

            def quoted(val:Any)->str:
                return f"'{val}'" if relation.lookup_attribute(remote_key).data_type.requires_quotes else val 

            constraint_set=[f" SELECT {quoted(val)} AS {remote_key} " for val in relation.data[remote_key].unique()]
            constraint_sql= ' UNION ALL '.join(constraint_set)       
            

        clause_string= f"""
{local_key} IN 
    ( SELECT  
        {remote_key}
    FROM (
{constraint_sql}
))
"""
        return clause_string


    def _sample_type_to_query_sql(self,sample_type:SampleType)->str:
        if isinstance(sample_type,BernoulliSample):
            return f"SAMPLE BERNOULLI ({sample_type.probability})"
        elif isinstance(sample_type,SystemSample):
            return f"SAMPLE SYSTEM ({sample_type.probability})"
        else:
            message=f"{sample_type.name} is not supported for SnowflakeAdapter"
            logger.error(message)
            raise NotImplementedError(message)

    def _build_conn_string(self)->str:
        """overrides the base conn string"""
        conn_parts=[f"snowflake://{self.credentials.user}:{self.credentials.password}@{self.credentials.account}/{self.credentials.database}/"]
        conn_parts.append(self.credentials.schema if self.credentials.schema is not None else '')
        get_args=list()
        for arg in ('warehouse','role',):
            if self.credentials.__dict__[arg] is not None:
                get_args.append(f"{arg}={self.credentials.__dict__[arg]}")
        
        get_string = "?" + "&".join([arg for arg in get_args])
        return (''.join(conn_parts)) + get_string  


    def get_relations_from_database(self,database:str)->List[Relation]:
        relations_sql=f"""
                                 SELECT 
                                    m.table_schema AS schema, 
                                    m.table_name AS relation, 
                                    m.table_type AS materialization,
                                    c.column_name AS attribute,
                                    c.ordinal_position AS ordinal,
                                    c.data_type AS data_type
                                 FROM 
                                    "{database}"."INFORMATION_SCHEMA"."TABLES" m
                                 INNER JOIN
                                    "{database}"."INFORMATION_SCHEMA"."COLUMNS" c  
                                 ON 
                                    c.table_schema = m.table_schema
                                 AND
                                    c.table_name = m.table_name
                                 WHERE
                                    m.table_schema <> 'INFORMATION_SCHEMA'
                              """
                                            
            
        logger.debug(f'Collecting detailed relations from database {database}...')
        relations_frame=self._safe_query(relations_sql)
        unique_relations = (relations_frame['schema'] +'.'+relations_frame['relation']).unique().tolist()
        logger.debug(f'Done collecting relations. Found a total of {len(unique_relations)} unique relations in database {database}')
        relations=list()
        for relation in unique_relations:
            logger.debug(f'Building relation { database + "." + relation }...')
            attributes=list()

            for attribute in relations_frame.loc[(relations_frame['schema']+'.'+relations_frame['relation']) == relation].itertuples():
                logger.debug(f'adding attribute {attribute.attribute} to relation..')
                attributes.append(
                            Attribute(
                                attribute.attribute,
                                self._get_data_type(attribute.data_type)
                                ))
            
            relation=Relation(database,
                              attribute.schema,
                              attribute.relation,
                              self.MATERIALIZATION_MAPPINGS[attribute.materialization],
                              attributes)
            logger.debug(f'Added relation {relation.dot_notation} to pool.')
            relations.append(relation)

        logger.debug(f'Acquired {len(relations)} total relations from database {database}.')
        return relations 
        

    def _count_query(self,query:str)->int:
        count_sql=f"WITH __SNOWSHU__COUNTABLE__QUERY as ({query}) SELECT COUNT(*) AS count FROM __SNOWSHU__COUNTABLE__QUERY"
        count=int(self._safe_query(count_sql).iloc[0]['count'])
        return count
            
    def check_count_and_query(self,query:str,max_count:int)->pd.DataFrame:
        """ checks the count, if count passes returns results as a dataframe."""
        try:
            logger.debug('Checking count for query...')
            start_time = time.time()
            count=self._count_query(query)
            assert count <= max_count
            logger.debug(f'Query count safe at {count} rows in {time.time()-start_time} seconds.')
        except AssertionError:
            message=f'failed to execute query, result would have returned {count} rows but the max allowed rows for this type of query is {max_count}.'
            logger.error(message)
            logger.debug(f'failed sql: {query}')
            raise ValueError(message)
        response=self._safe_query(query)
        return response
