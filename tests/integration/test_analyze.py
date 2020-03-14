import os
import pytest
import docker
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.configs import PACKAGE_ROOT
from snowshu.core.docker import SnowShuDocker

def test_analyze_unsampled(docker_flush):

    replica = ReplicaFactory()
    replica_files_dir = os.path.join(PACKAGE_ROOT,
                                     "tests",
                                     "assets",
                                     "integration",
                                     "replica_files")

    for path in os.listdir(replica_files_dir):
        if path.endswith('_replica.yml'):
            config=os.path.join(replica_files_dir,path)
            replica.load_config(config)
            result = replica.analyze(barf=False).split('\n')
            result.reverse()
            for line in result:
                if "ORDERS" in line:
                    assert '\x1b[0;32m100 %\x1b[0m' in line
                    break
