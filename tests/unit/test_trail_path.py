import sqlalchemy
import mock
import os
from snowshu.utils import PACKAGE_ROOT
from tests.common import rand_string
import pytest
import yaml
from io import StringIO
from snowshu.source_adapters import SnowflakeAdapter
from snowshu.core.trail_path import TrailPath
from dataclasses import dataclass

@pytest.fixture
def randomized_config():
    with open(os.path.join(PACKAGE_ROOT,'tests','assets','unit','trail-path-v1.yml'),'r') as config:
        test_trail_path=yaml.safe_load(config)
    
    @dataclass
    class Expected:
        DRELATION:str
        BIRELATION:str
        DRELATION_SCHEMA:str 
        BIRELATION_SCHEMA:str
        BIRELATION_LOCAL_ATTRIBUTE:str
        BIRELATION_REMOTE_ATTRIBUTE:str
        DRELATION_LOCAL_ATTRIBUTE:str
        DRELATION_REMOTE_ATTRIBUTE:str

        ## creds
        SOURCES_NAME:str
        SOURCES_ADAPTER:str
        SOURCES_ACCOUNT:str
        SOURCES_PASSWORD:str
        SOURCES_USERNAME:str
        
        TARGETS_NAME:str
        TARGETS_ADAPTER:str
        TARGETS_ACCOUNT:str
        TARGETS_PASSWORD:str
        TARGETS_USERNAME:str


        STORAGES_NAME:str
        STORAGES_ADAPTER:str
        STORAGES_ACCOUNT:str
        STORAGES_PASSWORD:str
        STORAGES_USERNAME:str
        CREDPATH:str=f"{PACKAGE_ROOT}/tests/assets/unit/credentials.yml"
    
    expected=Expected(*[rand_string(10) for _ in range(23)])

   
    # randomize values
    relationships=test_trail_path['source']['relations'][0]['relationships']
    directional=relationships['depends_on'][0]
    directional['local_attribute']=expected.DRELATION_LOCAL_ATTRIBUTE
    directional['remote_attribute']=expected.DRELATION_REMOTE_ATTRIBUTE
    directional['relation']=expected.DRELATION
    directional['schema']=expected.DRELATION_SCHEMA
    bidirectional=relationships['bidirectional'][0]
    bidirectional['local_attribute']=expected.BIRELATION_LOCAL_ATTRIBUTE
    bidirectional['remote_attribute']=expected.BIRELATION_REMOTE_ATTRIBUTE
    bidirectional['relation']=expected.BIRELATION
    bidirectional['schema']=expected.BIRELATION_SCHEMA
    relationships['depends_on'][0]=directional
    relationships['bidirectional'][0]=bidirectional
    test_trail_path['source']['relations'][0]['relationships']=relationships
    test_trail_path['credpath']=expected.CREDPATH

    as_dict=test_trail_path
    as_file_object=StringIO(yaml.dump(as_dict))

    random_creds_as_dict = dict( version="2",
          sources=[dict(
                        name=expected.SOURCES_NAME,
                        adapter=expected.SOURCES_ADAPTER,
                        account=expected.SOURCES_ACCOUNT,
                        password=expected.SOURCES_PASSWORD,
                        username=expected.SOURCES_USERNAME)],
          targets=[dict(
                        name=expected.TARGETS_NAME,
                        adapter=expected.TARGETS_ADAPTER,
                        account=expected.TARGETS_ACCOUNT,
                        password=expected.TARGETS_PASSWORD,
                        username=expected.TARGETS_USERNAME)],
          storages=[dict(

                        name=expected.STORAGES_NAME,
                        adapter=expected.STORAGES_ADAPTER,
                        account=expected.STORAGES_ACCOUNT,
                        password=expected.STORAGES_PASSWORD,
                        username=expected.STORAGES_USERNAME)])
    random_creds_as_file_object=StringIO(yaml.dump(random_creds_as_dict))

    yield as_file_object, as_dict, expected, random_creds_as_file_object, random_creds_as_dict,




def test_loads_correct_cred_profiles(randomized_config):
    config,_,__,___,____=randomized_config
    tp=TrailPath()
    tp.load_config(config)
    
    #assert tp._credentials['source']['password'] == 'super-secure-password'
   
def test_errors_on_bad_profile(randomized_config):
    _,config_dict,expected,__,___=randomized_config
    tp=TrailPath()
    SOURCE_PROFILE,TARGET_PROFILE,STORAGE_PROFILE=[rand_string(10) for _ in range(3)]
    config_dict['source']['profile']=SOURCE_PROFILE
    config_dict['target']['profile']=TARGET_PROFILE
    config_dict['storage']['profile']=STORAGE_PROFILE
    with pytest.raises(AttributeError):
        tp.load_config(StringIO(yaml.dump(config_dict)))


def test_loads_good_creds(randomized_config):
    config,_,expected,creds,__ =randomized_config
    trail_path=TrailPath()
    
    trail_path._load_credentials(creds,
                                 expected.SOURCES_NAME,
                                 expected.TARGETS_NAME,
                                 expected.STORAGES_NAME)
    assert trail_path._credentials['source']['password'] == expected.SOURCES_PASSWORD
    assert trail_path._credentials['target']['username'] == expected.TARGETS_USERNAME
    assert trail_path._credentials['storage']['account'] == expected.STORAGES_ACCOUNT
        
def test_sets_good_source_adapter(randomized_config):
    config,_,expected,__,___ = randomized_config
    trail_path=TrailPath()
    trail_path.load_config(config)

    assert isinstance(trail_path.source_adapter,SnowflakeAdapter)

def test_rejects_bad_adapter():
    trail_path=TrailPath()
    with pytest.raises(KeyError):
        trail_path._fetch_source_adapter('european_plug_adapter')

def test_get_connection(randomized_config):
    config,_,expected,__,___ = randomized_config
    trail_path=TrailPath()
    USER,PASSWORD,ACCOUNT,DATABASE,SCHEMA,ROLE = [rand_string(10) for _ in range(6)]
    
    trail_path.source_adapter=SnowflakeAdapter()
    trail_path._credentials=dict(source=dict(user=USER,
                                             password=PASSWORD,
                                             account=ACCOUNT,
                                             database=DATABASE,
                                             schema=SCHEMA,
                                             role=ROLE))
    
    trail_path._set_connections()
    assert isinstance(trail_path.connections['source'],sqlalchemy.engine.base.Engine)
          
    
